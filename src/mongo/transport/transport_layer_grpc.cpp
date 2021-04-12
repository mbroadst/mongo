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

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

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

    /*
    absl::flat_hash_map<std::string, GRPCSessionHandle>::iterator sit = _tl->_sessions.find(lcid);
    GRPCSessionHandle session;
    if (sit == _tl->_sessions.end()) {
        stdx::lock_guard<Latch> lk(_tl->_mutex);
        auto result = _tl->_sessions.emplace(lcid, std::make_shared<GRPCSession>(_tl, lcid));
        if (!result.second) {
            reactor->Finish(
                grpc::Status(grpc::INVALID_ARGUMENT, "unable to create server session"));
            return reactor;
        }

        session = result.first->second;

        // TODO: how are these ever removed!
        _tl->_sep->startSession(session);
    } else {
        session = sit->second;
    }
    */

    std::pair<absl::flat_hash_map<std::string, GRPCSessionHandle>::iterator, bool> result;
    {
        stdx::lock_guard<Latch> lk(_tl->_mutex);
        result = _tl->_sessions.try_emplace(
            lcid, lazy_convert_construct([&] { return std::make_shared<GRPCSession>(_tl, lcid);
            }));
    }

    auto session = result.first->second;
    session->_pendingRequests.push(new GRPCSession::PendingRequest{request, response, reactor});

    if (result.second) {
        // TODO: how are these ever removed!
        _tl->_sep->startSession(session);
    }

    return reactor;
}

void TransportLayerGRPC::GRPCSession::end() {
    auto [requests, bytes] = _pendingRequests.popMany();
    for (auto request : requests) {
        request->reactor->Finish(grpc::Status::CANCELLED);
    }
    requests.clear();
}

StatusWith<Message> TransportLayerGRPC::GRPCSession::sourceMessage() {
    _currentRequest = _pendingRequests.pop();
    auto requestMessage = _currentRequest->request->payload();
    auto buffer = SharedBuffer::allocate(requestMessage.size());
    memcpy(buffer.get(), requestMessage.data(), requestMessage.size());
    return Message(std::move(buffer));
}

Status TransportLayerGRPC::GRPCSession::sinkMessage(Message message) {
    _currentRequest->response->set_payload(std::string(message.buf(), message.size()));
    _currentRequest->reactor->Finish(grpc::Status::OK);

    delete _currentRequest;
    _currentRequest = nullptr;

    return Status::OK();
}

TransportLayerGRPC::TransportLayerGRPC(ServiceEntryPoint* sep) : _sep(sep), _service(this) {}

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
    _sessions.clear();
    _server->Shutdown();
}

ReactorHandle TransportLayerGRPC::getReactor(WhichReactor which) {
    return nullptr;
}

TransportLayerGRPC::~TransportLayerGRPC() {
    shutdown();
}

}  // namespace transport
}  // namespace mongo
