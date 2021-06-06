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

#include <absl/container/flat_hash_map.h>

#include "mongo/base/status.h"
#include "mongo/transport/transport_layer.h"
#include "mongo/util/net/ssl_types.h"
#include "mongo/util/time_support.h"

namespace grpc {
class Server;
class Channel;
}  // namespace grpc

namespace mongo {
namespace transport {

class TransportLayerGRPC : public TransportLayer {
    TransportLayerGRPC(const TransportLayerGRPC&) = delete;
    TransportLayerGRPC& operator=(const TransportLayerGRPC&) = delete;

public:
    struct Options {
        constexpr static auto kIngress = 0x1;
        constexpr static auto kEgress = 0x10;

        explicit Options(const ServerGlobalParams* params);
        explicit Options(const std::vector<std::string>& ipList, int port);
        Options() = default;

        bool isIngress() const {
            return mode & kIngress;
        }

        bool isEgress() const {
            return mode & kEgress;
        }

        int mode = kIngress | kEgress;
        std::vector<std::string> ipList;               // addresses to bind to
        int port = ServerGlobalParams::DefaultDBPort;  // port to bind to
    };

    TransportLayerGRPC(const Options& options,
                       ServiceEntryPoint* sep,
                       const WireSpec& wireSpec = WireSpec::instance());
    ~TransportLayerGRPC();

    StatusWith<SessionHandle> connect(HostAndPort peer,
                                      ConnectSSLMode sslMode,
                                      Milliseconds timeout,
                                      boost::optional<TransientSSLParams> transientSSLParams) final;

    Future<SessionHandle> asyncConnect(
        HostAndPort peer,
        ConnectSSLMode sslMode,
        const ReactorHandle& reactor,
        Milliseconds timeout,
        std::shared_ptr<const SSLConnectionContext> transientSSLContext = nullptr) final;

    Status setup() final;
    Status start() final;
    void shutdown() final;

    virtual ReactorHandle getReactor(WhichReactor which) final;

#ifdef MONGO_CONFIG_SSL
    /** Rotate the in-use certificates for new connections. */
    virtual Status rotateCertificates(std::shared_ptr<SSLManagerInterface> manager,
                                      bool asyncOCSPStaple) final {
        // UNIMPLEMENTED
        return Status::OK();
    }

    /**
     * Creates a transient SSL context using targeted (non default) SSL params.
     * @param transientSSLParams overrides any value in stored SSLConnectionContext.
     * @param optionalManager provides an optional SSL manager, otherwise the default one will be
     * used.
     */
    virtual StatusWith<std::shared_ptr<const transport::SSLConnectionContext>>
    createTransientSSLContext(const TransientSSLParams& transientSSLParams) final {
        // UNIMPLEMENTED
        return Status::OK();
    }
#endif

private:
    class TransportServiceImpl;
    class GRPCSession;
    class GRPCEgressSession;

    using GRPCSessionHandle = std::shared_ptr<GRPCSession>;
    GRPCSessionHandle getLogicalSessionHandle(const std::string& lcid);

    Options _options;
    ServiceEntryPoint* const _sep = nullptr;
    stdx::thread _thread;
    std::unique_ptr<grpc::Server> _server;
    std::unique_ptr<TransportServiceImpl> _service;

#ifdef MONGO_CONFIG_SSL
    synchronized_value<std::shared_ptr<const SSLConnectionContext>> _sslContext;
#endif

    Mutex _mutex = MONGO_MAKE_LATCH("TransportLayerGRPC::_mutex");
    absl::flat_hash_map<std::string, GRPCSessionHandle> _sessions;
    absl::flat_hash_map<HostAndPort, std::shared_ptr<grpc::Channel>> _channels;
};

}  // namespace transport
}  // namespace mongo
