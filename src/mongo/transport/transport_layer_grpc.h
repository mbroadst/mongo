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

#pragma once

#define GRPC_CALLBACK_API_NONEXPERIMENTAL 1
#include <grpcpp/grpcpp.h>

#include "mongo/base/status.h"
#include "mongo/transport/transport_layer.h"
#include "mongo/util/net/ssl_types.h"
#include "mongo/util/time_support.h"

#include "mongo/transport/mongodb.grpc.pb.h"
#include "mongo/transport/mongodb.pb.h"

namespace mongo {
namespace transport {

class TransportLayerGRPC : public TransportLayer {
    TransportLayerGRPC(const TransportLayerGRPC&) = delete;
    TransportLayerGRPC& operator=(const TransportLayerGRPC&) = delete;

public:
    TransportLayerGRPC(ServiceEntryPoint* sep);
    ~TransportLayerGRPC();

    StatusWith<SessionHandle> connect(HostAndPort peer,
                                      ConnectSSLMode sslMode,
                                      Milliseconds timeout) override;
    Future<SessionHandle> asyncConnect(HostAndPort peer,
                                       ConnectSSLMode sslMode,
                                       const ReactorHandle& reactor,
                                       Milliseconds timeout) override;

    Status setup() override;
    Status start() override;
    void shutdown() override;

    virtual ReactorHandle getReactor(WhichReactor which) override;

private:
    class TransportServiceImpl final : public mongodb::Transport::CallbackService {
        grpc::ServerUnaryReactor* SendMessage(grpc::CallbackServerContext* context,
                                              const mongodb::Message* request,
                                              mongodb::Message* response) override;
    };

    ServiceEntryPoint* const _sep;
    stdx::thread _thread;
    std::unique_ptr<grpc::Server> _server;
    TransportServiceImpl _service;

};

}  // namespace transport
}  // namespace mongo
