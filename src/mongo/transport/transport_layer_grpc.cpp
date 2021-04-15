/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/transport/transport_layer_grpc.h"
#include "mongo/transport/service_entry_point.h"

#include "mongo/platform/basic.h"

#include "mongo/db/stats/counters.h"

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <fmt/format.h>

#include <memory>

namespace mongo {
namespace transport {

// borrowed from https://jguegant.github.io/blogs/tech/performing-try-emplace.html
template <class Factory>
struct lazy_convert_construct {
    using result_type = std::invoke_result_t<const Factory&>;

    constexpr lazy_convert_construct(Factory&& factory) : factory_(std::move(factory)) {}
    constexpr operator result_type() const noexcept(std::is_nothrow_invocable_v<const Factory&>) {
        return factory_();
    }

    Factory factory_;
};

std::mutex g_display_mutex;
void threadLog(const std::string& message) {
    std::thread::id this_id = std::this_thread::get_id();
    g_display_mutex.lock();
    std::cout << "thread(" << this_id << "): " << message << "\n";
    g_display_mutex.unlock();
}

int32_t TransportLayerGRPC::TransportServiceImpl::connCounter = 0;
TransportLayerGRPC::TransportServiceImpl::TransportServiceImpl(TransportLayerGRPC* tl) : _tl(tl) {}
grpc::ServerUnaryReactor* TransportLayerGRPC::TransportServiceImpl::SendMessage(
    grpc::CallbackServerContext* context,
    const mongodb::Message* request,
    mongodb::Message* response) {
    grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
    const auto clientMetadata = context->client_metadata();
    auto it = clientMetadata.find("lcid");
    if (it == clientMetadata.end()) {
        reactor->Finish(
            grpc::Status(grpc::INVALID_ARGUMENT, "missing required logical connection id"));
        return reactor;
    }

    auto lcid = std::string(it->second.begin(), it->second.end());
    // threadLog(fmt::format("[{}] processing incoming message", lcid));

    auto worker = _tl->getLogicalConnectionWorker(lcid);
    // threadLog(fmt::format("[{}] sending to handle request", lcid));
    worker->handleRequest(request, response, reactor);

    // threadLog(fmt::format("[{}] returned reactor", lcid));
    return reactor;
}

TransportLayerGRPC::Worker::Worker(ServiceContext* svcContext, transport::SessionHandle session)
    : _sep{svcContext->getServiceEntryPoint()},
      _serviceContext(svcContext),
      _serviceExecutor(_serviceContext->getServiceExecutor()),
      _sessionHandle(session),
      _threadName{str::stream() << "grpc" << session->id()},
      _dbClient{svcContext->makeClient(_threadName, std::move(session))} {
}

void TransportLayerGRPC::Worker::handleRequest(const mongodb::Message* request,
                                               mongodb::Message* response,
                                               grpc::ServerUnaryReactor* reactor) {
    // convert the request payload into a message
    auto requestMessage = request->payload();
    auto buffer = SharedBuffer::allocate(requestMessage.size());
    memcpy(buffer.get(), requestMessage.data(), requestMessage.size());
    networkCounter.hitPhysicalIn(requestMessage.size());
    Message message(std::move(buffer));

    // this is needed for calls to Client::cc() elsewhere in the code
    Client::setCurrent(std::move(_dbClient));

    // create a new opCtx and run the request against the server
    auto opCtx = Client::getCurrent()->makeOperationContext();
    DbResponse dbresponse = _sep->handleRequest(opCtx.get(), message);
    opCtx.reset();

    Message& toSink = dbresponse.response;
    // TODO: there is a whole bunch of stuff chopped out here (compression, exhaust, etc)

    if (toSink.empty()) {
        _dbClient = Client::releaseCurrent();
        reactor->Finish(grpc::Status::OK);
        return;
    }

    invariant(!OpMsg::isFlagSet(message, OpMsg::kMoreToCome));
    invariant(!OpMsg::isFlagSet(toSink, OpMsg::kChecksumPresent));

    // Update the header for the response message.
    toSink.header().setId(nextMessageId());
    toSink.header().setResponseToMsgId(message.header().getId());
    if (OpMsg::isFlagSet(message, OpMsg::kChecksumPresent)) {
        OpMsg::appendChecksum(&toSink);
    }

    _dbClient = Client::releaseCurrent();

    networkCounter.hitPhysicalOut(toSink.size());
    response->set_payload(std::string(toSink.buf(), toSink.size()));
    reactor->Finish(grpc::Status::OK);
}

TransportLayerGRPC::TransportLayerGRPC(ServiceEntryPoint* sep, ServiceContext* ctx)
    : _sep(sep), _ctx(ctx), _service(this) {}

StatusWith<SessionHandle> TransportLayerGRPC::connect(HostAndPort peer,
                                                      ConnectSSLMode sslMode,
                                                      Milliseconds timeout) {
    MONGO_UNREACHABLE;
}

Future<SessionHandle> TransportLayerGRPC::asyncConnect(HostAndPort peer,
                                                       ConnectSSLMode sslMode,
                                                       const ReactorHandle& reactor,
                                                       Milliseconds timeout) {
    MONGO_UNREACHABLE;
}

Status TransportLayerGRPC::setup() {
    return Status::OK();
}

Status TransportLayerGRPC::start() {
    _thread = stdx::thread([this] {
        grpc::EnableDefaultHealthCheckService(true);
        grpc::reflection::InitProtoReflectionServerBuilderPlugin();

        grpc::ServerBuilder builder;
        builder.AddListeningPort("0.0.0.0:50051", grpc::InsecureServerCredentials());
        builder.RegisterService(&_service);
        _server = builder.BuildAndStart();
        _server->Wait();
    });

    return Status::OK();
}

void TransportLayerGRPC::shutdown() {
    _workers.clear();
    _server->Shutdown();
}

ReactorHandle TransportLayerGRPC::getReactor(WhichReactor which) {
    return nullptr;
}

TransportLayerGRPC::~TransportLayerGRPC() {
    shutdown();
}

TransportLayerGRPC::Worker* TransportLayerGRPC::getLogicalConnectionWorker(
    const std::string& lcid) {
    // NOTE: maybe less lock contention with this approach, but requires double lookup for session
    absl::flat_hash_map<std::string, WorkerPtr>::iterator sit = _workers.find(lcid);
    if (sit == _workers.end()) {
        stdx::lock_guard<Latch> lk(_mutex);
        auto session = std::make_shared<GRPCSession>(this, lcid);
        auto [entry, emplaced] = _workers.emplace(lcid, new Worker(_ctx, std::move(session)));
        if (!emplaced) {
            // throw an exception?
        }

        return entry->second.get();
    }

    return sit->second.get();
}

}  // namespace transport
}  // namespace mongo
