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

TransportLayerGRPC::TransportServiceImpl::TransportServiceImpl(TransportLayerGRPC* tl) : _tl(tl) {}
grpc::ServerUnaryReactor* TransportLayerGRPC::TransportServiceImpl::SendMessage(
    grpc::CallbackServerContext* context,
    const mongodb::Message* request,
    mongodb::Message* response) {
    grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
    const auto clientMetadata = context->client_metadata();
    auto it = clientMetadata.find("lcid");
    if (it == clientMetadata.end()) {
        reactor->Finish(grpc::Status(grpc::INVALID_ARGUMENT, "missing required logical connection id"));
        return reactor;
    }

    auto lcid = std::string(it->second.begin(), it->second.end());
    {
        stdx::lock_guard<Latch> lk(_tl->_mutex);
        auto [pair, emplaced] = _tl->_sessions.try_emplace(lcid, new GRPCSession(_tl));
        auto session = pair->second;
        session->addRequest(request, response, reactor);

        // if this is a new session, then we need to actually start it
        if (emplaced) {
            // TODO: how are these ever removed!
            std::cout << "new sessions started for lcid: " << lcid << std::endl;
            _tl->_sep->startSession(std::move(session));
        }
    }

    return reactor;
}

void TransportLayerGRPC::GRPCSession::end() {
    _ended = true;

    for (auto request : _pendingRequests) {
        request.reactor->Finish(grpc::Status::CANCELLED);
    }
    _pendingRequests.clear();

    for (auto [id, request] : _activeRequests) {
        request.reactor->Finish(grpc::Status::CANCELLED);
    }
    _activeRequests.clear();

    // notify waiters that we have ended
    _waitForPending.notify_one();
}

void TransportLayerGRPC::GRPCSession::addRequest(const mongodb::Message* request,
                                                 mongodb::Message* response,
                                                 grpc::ServerUnaryReactor* reactor) {
    {
        stdx::lock_guard<Latch> lk(_mutex);
        _pendingRequests.emplace_back(PendingRequest{request, response, reactor});
    }

    _waitForPending.notify_one();
}

StatusWith<Message> TransportLayerGRPC::GRPCSession::sourceMessage() {
    std::unique_lock<Latch> ul(_mutex);
    _waitForPending.wait(ul, [this] { return _pendingRequests.size() > 0 || _ended; });
    if (_ended) {
        return Message();
    }

    auto request = _pendingRequests.front().request;
    auto requestMessage = request->payload();
    auto buffer = SharedBuffer::allocate(requestMessage.size());
    memcpy(buffer.get(), requestMessage.data(), requestMessage.size());

    auto message = Message(std::move(buffer));
    _activeRequests.emplace(message.header().getId(), std::move(_pendingRequests.front()));
    _pendingRequests.pop_front();
    return message;
}

Status TransportLayerGRPC::GRPCSession::sinkMessage(Message message) {
    auto id = message.header().getResponseToMsgId();
    if (!_activeRequests.contains(id)) {
        return TransportLayer::SessionUnknownStatus;
    }

    stdx::lock_guard<Latch> lk(_mutex);
    auto [request, response, reactor] = _activeRequests[id];
    _activeRequests.erase(id);
    response->set_payload(std::string(message.buf(), message.size()));
    reactor->Finish(grpc::Status::OK);
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
