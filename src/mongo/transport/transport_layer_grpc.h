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

#include <absl/container/flat_hash_map.h>

#include "mongo/base/status.h"
#include "mongo/transport/transport_layer.h"
#include "mongo/util/net/ssl_types.h"
#include "mongo/util/producer_consumer_queue.h"
#include "mongo/util/time_support.h"

#include "mongo/transport/mongodb.grpc.pb.h"
#include "mongo/transport/mongodb.pb.h"

namespace mongo {
namespace transport {

class TransportLayerGRPC : public TransportLayer {
    TransportLayerGRPC(const TransportLayerGRPC&) = delete;
    TransportLayerGRPC& operator=(const TransportLayerGRPC&) = delete;

public:
    TransportLayerGRPC(ServiceEntryPoint* sep, ServiceContext* ctx);
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
    public:
        explicit TransportServiceImpl(TransportLayerGRPC* transportLayer);

    private:
        grpc::ServerUnaryReactor* SendMessage(grpc::CallbackServerContext* context,
                                              const mongodb::Message* request,
                                              mongodb::Message* response) override;

        TransportLayerGRPC* _tl;
    };

    class GRPCSession : public Session {
        GRPCSession(const GRPCSession&) = delete;
        GRPCSession& operator=(const GRPCSession&) = delete;

    public:
        explicit GRPCSession(TransportLayer* tl, const std::string& lcid)
            : _tl(checked_cast<TransportLayerGRPC*>(tl)), _lcid(lcid) {
            setTags(kDefaultBatonHack);
        }

        ~GRPCSession() {
            end();
        }

        TransportLayer* getTransportLayer() const override {
            return _tl;
        }

        const HostAndPort& remote() const override {
            return _remote;
        }

        const HostAndPort& local() const override {
            return _local;
        }

        const SockAddr& remoteAddr() const override {
            return _remoteAddr;
        }

        const SockAddr& localAddr() const override {
            return _localAddr;
        }

        void end() override {}
        StatusWith<Message> sourceMessage() override {
            MONGO_UNREACHABLE;
        }
        Future<Message> asyncSourceMessage(const BatonHandle& handle = nullptr) override {
            return Future<Message>::makeReady(sourceMessage());
        }

        Status sinkMessage(Message message) override {
            MONGO_UNREACHABLE;
        }
        Future<void> asyncSinkMessage(Message message,
                                      const BatonHandle& handle = nullptr) override {
            return Future<void>::makeReady(sinkMessage(message));
        }

        // TODO: do we need these?
        void cancelAsyncOperations(const BatonHandle& handle = nullptr) override {}
        void setTimeout(boost::optional<Milliseconds>) override {}
        bool isConnected() override {
            return true;
        }

    protected:
        TransportLayer* _tl;
        std::string _lcid;
        HostAndPort _remote{};
        HostAndPort _local{};
        SockAddr _remoteAddr;
        SockAddr _localAddr;
    };

    class Worker : public std::enable_shared_from_this<Worker> {
        Worker(const Worker&) = delete;
        Worker& operator=(const Worker&) = delete;

    public:
        explicit Worker(ServiceContext* svcContext, transport::SessionHandle session);
        void handleRequest(const mongodb::Message* request,
                           mongodb::Message* response,
                           grpc::ServerUnaryReactor* reactor);

    private:
        ServiceEntryPoint* _sep;
        const std::string _threadName;
        ServiceContext::UniqueClient _dbClient;

    };

    using WorkerPtr = std::unique_ptr<Worker>;
    Worker* getLogicalConnectionWorker(const std::string& lcid);

    ServiceEntryPoint* const _sep = nullptr;
    ServiceContext* const _ctx = nullptr;
    stdx::thread _thread;
    std::unique_ptr<grpc::Server> _server;
    TransportServiceImpl _service;

    Mutex _mutex = MONGO_MAKE_LATCH("TransportLayerGRPC::_mutex");
    absl::flat_hash_map<std::string, WorkerPtr> _workers;
};

}  // namespace transport
}  // namespace mongo
