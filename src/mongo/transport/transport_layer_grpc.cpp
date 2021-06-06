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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kNetwork

#define GRPC_CALLBACK_API_NONEXPERIMENTAL 1
#include <grpcpp/grpcpp.h>

#include "mongo/transport/mongodb.grpc.pb.h"
#include "mongo/transport/mongodb.pb.h"
#include "mongo/transport/reactor_asio.h"
#include "mongo/transport/service_entry_point.h"
#include "mongo/transport/transport_layer_asio.h"
#include "mongo/transport/transport_layer_grpc.h"

#include "mongo/db/stats/counters.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/util/producer_consumer_queue.h"

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

class TransportLayerGRPC::TransportServiceImpl final : public mongodb::Transport::CallbackService {
public:
    explicit TransportServiceImpl(TransportLayerGRPC* transportLayer);

private:
    grpc::ServerUnaryReactor* SendMessage(grpc::CallbackServerContext* context,
                                          const mongodb::Message* request,
                                          mongodb::Message* response) override;

    TransportLayerGRPC* _tl;
};

class TransportLayerGRPC::GRPCSession : public Session {
    GRPCSession(const GRPCSession&) = delete;
    GRPCSession& operator=(const GRPCSession&) = delete;

public:
    explicit GRPCSession(TransportLayerGRPC* tl,
                         const std::string& lcid,
                         std::shared_ptr<const SSLConnectionContext> transientSSLContext = nullptr)
        : _tl(checked_cast<TransportLayerGRPC*>(tl)), _lcid(lcid) {
        setTags(kDefaultBatonHack);
        _sslContext = transientSSLContext ? transientSSLContext : *tl->_sslContext;
    }

    ~GRPCSession() {
        end();
    }

    TransportLayer* getTransportLayer() const override {
        return _tl;
    }

    const std::string& lcid() const {
        return _lcid;
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

    void end() override;
    StatusWith<Message> sourceMessage() noexcept override;
    Future<Message> asyncSourceMessage(const BatonHandle& handle = nullptr) noexcept override {
        return Future<Message>::makeReady(sourceMessage());
    }

    Status waitForData() noexcept override;
    Future<void> asyncWaitForData() noexcept override {
        return Future<void>::makeReady(waitForData());
    }

    Status sinkMessage(Message message) noexcept override;
    Future<void> asyncSinkMessage(Message message,
                                  const BatonHandle& handle = nullptr) noexcept override {
        return Future<void>::makeReady(sinkMessage(message));
    }

    // TODO: do we need these?
    void cancelAsyncOperations(const BatonHandle& handle = nullptr) override {}
    void setTimeout(boost::optional<Milliseconds>) override {}
    bool isConnected() override {
        return true;
    }

#ifdef MONGO_CONFIG_SSL
    /**
     * Get the configuration from the SSL manager.
     */
    const SSLConfiguration* getSSLConfiguration() const override {
        if (_sslContext->manager) {
            return &_sslContext->manager->getSSLConfiguration();
        }

        return nullptr;
    }

    /**
     * Get the SSL manager associated with this session.
     */
    const std::shared_ptr<SSLManagerInterface> getSSLManager() const override {
        return _sslContext->manager;
    }
#endif

protected:
    friend class TransportLayerGRPC::TransportServiceImpl;
    TransportLayerGRPC* _tl;
    std::string _lcid;

    HostAndPort _remote{};
    HostAndPort _local{};
    SockAddr _remoteAddr;
    SockAddr _localAddr;

#ifdef MONGO_CONFIG_SSL
    std::shared_ptr<const SSLConnectionContext> _sslContext;
#endif

    struct PendingRequest {
        const mongodb::Message* request;
        mongodb::Message* response;
        grpc::ServerUnaryReactor* reactor;
    };

    Mutex _mutex = MONGO_MAKE_LATCH("GRPCSession::_mutex");
    // TODO: using PendingRequestPtr = std::unique_ptr<PendingRequest>;
    MultiProducerMultiConsumerQueue<PendingRequest*> _pendingRequests;
    PendingRequest* _currentRequest = nullptr;
};

class TransportLayerGRPC::GRPCEgressSession : public Session {
    GRPCEgressSession(const GRPCEgressSession&) = delete;
    GRPCEgressSession& operator=(const GRPCEgressSession&) = delete;

public:
    explicit GRPCEgressSession(
        HostAndPort peer,
        TransportLayerGRPC* tl,
        std::shared_ptr<grpc::Channel> channel,
        std::shared_ptr<const SSLConnectionContext> transientSSLContext = nullptr)
        : _tl(checked_cast<TransportLayerGRPC*>(tl)),
          _lcid(UUID::gen().toString()),
          _remote(peer),
          _stub(mongodb::Transport::NewStub(channel)) {
        setTags(kDefaultBatonHack);
        _sslContext = transientSSLContext ? transientSSLContext : *tl->_sslContext;
    }

    ~GRPCEgressSession() {
        end();
    }

    TransportLayer* getTransportLayer() const override {
        return _tl;
    }

    const std::string& lcid() const {
        return _lcid;
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

    void end() override;
    StatusWith<Message> sourceMessage() noexcept override;
    Future<Message> asyncSourceMessage(const BatonHandle& handle = nullptr) noexcept override {
        return Future<Message>::makeReady(sourceMessage());
    }

    Status waitForData() noexcept override;
    Future<void> asyncWaitForData() noexcept override {
        return Future<void>::makeReady(waitForData());
    }

    Status sinkMessage(Message message) noexcept override {
        return asyncSinkMessage(message).getNoThrow();
    }
    Future<void> asyncSinkMessage(Message message,
                                  const BatonHandle& handle = nullptr) noexcept override;

    void cancelAsyncOperations(const BatonHandle& handle = nullptr) override {
        // LOGV2(99993, "cancel async operations");
    }

    void setTimeout(boost::optional<Milliseconds>) override {
        // invariant(!timeout || timeout->count() > 0);
        // _timeout = timeout;
    }

    bool isConnected() override {
        return true;
    }

#ifdef MONGO_CONFIG_SSL
    /**
     * Get the configuration from the SSL manager.
     */
    const SSLConfiguration* getSSLConfiguration() const override {
        if (_sslContext->manager) {
            return &_sslContext->manager->getSSLConfiguration();
        }

        return nullptr;
    }

    /**
     * Get the SSL manager associated with this session.
     */
    const std::shared_ptr<SSLManagerInterface> getSSLManager() const override {
        return _sslContext->manager;
    }
#endif

protected:
    TransportLayerGRPC* _tl;
    std::string _lcid;

    HostAndPort _remote{};
    HostAndPort _local{};
    SockAddr _remoteAddr;
    SockAddr _localAddr;

    std::unique_ptr<mongodb::Transport::Stub> _stub;
    MultiProducerSingleConsumerQueue<Message> _responses;

#ifdef MONGO_CONFIG_SSL
    std::shared_ptr<const SSLConnectionContext> _sslContext;
#endif

    struct PendingRequest {
        std::unique_ptr<mongodb::Message> request;
        std::unique_ptr<mongodb::Message> response;
        std::unique_ptr<grpc::ClientContext> context;
        Promise<void> promise;
    };

    Mutex _mutex = MONGO_MAKE_LATCH("GRPCEgressSession::_mutex");
    std::list<std::weak_ptr<PendingRequest>> _pendingRequests;
};

Message messageFromPayload(const std::string& payload) {
    auto requestBuffer = SharedBuffer::allocate(payload.size());
    memcpy(requestBuffer.get(), payload.data(), payload.size());
    return Message(std::move(requestBuffer));
}

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
    auto session = _tl->getLogicalSessionHandle(lcid);
    session->_pendingRequests.push(new GRPCSession::PendingRequest{request, response, reactor});
    return reactor;
}

void TransportLayerGRPC::GRPCSession::end() {
    auto [requests, bytes] = _pendingRequests.popMany();
    for (auto request : requests) {
        request->reactor->Finish(grpc::Status::CANCELLED);
    }
    requests.clear();
}

StatusWith<Message> TransportLayerGRPC::GRPCSession::sourceMessage() noexcept {
    _currentRequest = _pendingRequests.pop();
    auto requestMessage = messageFromPayload(_currentRequest->request->payload());
    networkCounter.hitPhysicalIn(requestMessage.size());
    return std::move(requestMessage);
}

Status TransportLayerGRPC::GRPCSession::waitForData() noexcept {
    _pendingRequests.waitForData();
    return Status::OK();
}

Status TransportLayerGRPC::GRPCSession::sinkMessage(Message message) noexcept {
    if (_currentRequest) {
        networkCounter.hitPhysicalOut(message.size());
        _currentRequest->response->set_payload(std::string(message.buf(), message.size()));
        _currentRequest->reactor->Finish(grpc::Status::OK);

        delete _currentRequest;
        _currentRequest = nullptr;
    }

    // TODO: what if there is no current request? we probably cancelled before the operation was
    // processed..
    return Status::OK();
}

void TransportLayerGRPC::GRPCEgressSession::end() {
    stdx::lock_guard<Latch> lk(_mutex);
    for (auto pendingRequest : _pendingRequests) {
        if (auto pr = pendingRequest.lock()) {
            pr->context->TryCancel();
        }
    }
    _pendingRequests.clear();
}

StatusWith<Message> TransportLayerGRPC::GRPCEgressSession::sourceMessage() noexcept {
    return _responses.pop();
}

Status TransportLayerGRPC::GRPCEgressSession::waitForData() noexcept {
    _responses.waitForData();
    return Status::OK();
}

Future<void> TransportLayerGRPC::GRPCEgressSession::asyncSinkMessage(
    Message message, const BatonHandle& handle) noexcept {
    auto pr = std::make_shared<PendingRequest>();
    pr->response = std::make_unique<mongodb::Message>();

    pr->request = std::make_unique<mongodb::Message>();
    pr->request->set_payload(std::string(message.buf(), message.size()));
    networkCounter.hitPhysicalOut(message.size());
    networkCounter.hitLogicalOut(message.size());

    pr->context = std::make_unique<grpc::ClientContext>();
    pr->context->AddMetadata("lcid", _lcid);

    // TODO: Cancellation might be a little inefficiently implemented here
    std::list<std::weak_ptr<PendingRequest>>::iterator it;
    {
        stdx::lock_guard<Latch> lk(_mutex);
        it = _pendingRequests.emplace(_pendingRequests.end(), pr);
    }

    auto pf = makePromiseFuture<void>();
    pr->promise = std::move(pf.promise);

    LOGV2(99996,
          "sending gRPC message({id}) to: {peer}",
          "id"_attr = message.header().getId(),
          "peer"_attr = _remote.toString());

    _stub->async()->SendMessage(
        pr->context.get(),
        pr->request.get(),
        pr->response.get(),
        [this, pr, message, it = std::move(it)](grpc::Status s) {
            // remove the pending request context
            {
                stdx::lock_guard<Latch> lk(_mutex);
                _pendingRequests.erase(it);
            }

            if (!s.ok()) {
                LOGV2(99995,
                      "completed gRPC message({id}) to: {peer}, error: {error}",
                      "id"_attr = message.header().getId(),
                      "peer"_attr = _remote.toString(),
                      "error"_attr = s.error_message());
                pr->promise.setError({ErrorCodes::InternalError, s.error_message()});
                return;
            }

            LOGV2(99994,
                  "completed gRPC message({id}) to: {peer}",
                  "id"_attr = message.header().getId(),
                  "peer"_attr = _remote.toString());
            auto message = messageFromPayload(pr->response->payload());
            networkCounter.hitPhysicalIn(message.size());
            networkCounter.hitLogicalIn(message.size());
            _responses.push(std::move(message));
            pr->promise.emplaceValue();
        });

    return std::move(pf.future);
}

TransportLayerGRPC::Options::Options(const ServerGlobalParams* params)
    : ipList(params->bind_ips), port(params->port) {}

TransportLayerGRPC::Options::Options(const std::vector<std::string>& ipList, int port)
    : ipList(ipList), port(port) {}

TransportLayerGRPC::TransportLayerGRPC(const Options& options,
                                       ServiceEntryPoint* sep,
                                       const WireSpec& wireSpec)
    : TransportLayer(wireSpec),
      _options(options),
      _sep(sep),
      _service(std::make_unique<TransportServiceImpl>(this)) {}

TransportLayerGRPC::~TransportLayerGRPC() {
    shutdown();
}

StatusWith<SessionHandle> TransportLayerGRPC::connect(
    HostAndPort peer,
    ConnectSSLMode sslMode,
    Milliseconds timeout,
    boost::optional<TransientSSLParams> transientSSLParams) {
    LOGV2(99998, "creating new egress connection to: {peer}", "peer"_attr = peer.toString());
    auto it = _channels.find(peer);
    auto channel = (it == _channels.end())
        ? grpc::CreateChannel(peer.toString(), grpc::InsecureChannelCredentials())
        : (*it).second;

    if (it == _channels.end()) {
        LOGV2(99997, "creating new channel for peer: {peer}", "peer"_attr = peer.toString());
        stdx::lock_guard<Latch> lk(_mutex);
        _channels[peer] = channel;
    }

    auto duration = std::chrono::system_clock::now() + timeout.toSystemDuration();
    if (!channel->WaitForConnected(duration)) {
        return {ErrorCodes::NetworkTimeout, "Timed out"};
    }

    SessionHandle session(new GRPCEgressSession(peer, this, channel));
    return std::move(session);
}

Future<SessionHandle> TransportLayerGRPC::asyncConnect(
    HostAndPort peer,
    ConnectSSLMode sslMode,
    const ReactorHandle& reactor,
    Milliseconds timeout,
    std::shared_ptr<const SSLConnectionContext> transientSSLContext) {
    return Future<SessionHandle>::makeReady(connect(peer, sslMode, timeout, boost::none));
}

Status TransportLayerGRPC::setup() {
    return Status::OK();
}

Status TransportLayerGRPC::start() {
    if (_options.isIngress()) {
        _thread = stdx::thread([this] {
            setThreadName("grpcListener");

            std::set<std::string> listenAddrs;
            if (_options.ipList.empty() && _options.isIngress()) {
                listenAddrs = {fmt::format("127.0.0.1:{}", _options.port)};
            } else if (!_options.ipList.empty()) {
                for (auto& ip : _options.ipList) {
                    if (ip.empty()) {
                        LOGV2_WARNING(23020, "Skipping empty bind address");
                        continue;
                    }

                    auto addr = uassertStatusOK(HostAndPort::parse(ip));
                    auto addrStr = fmt::format("{}:{}", addr.host(), addr.port());
                    LOGV2(99991,
                          "emplacing address: {a}, source: {s}",
                          "a"_attr = addrStr,
                          "s"_attr = ip);
                    listenAddrs.emplace(fmt::format("{}:{}", addr.host(), addr.port()));
                }
            }

            grpc::EnableDefaultHealthCheckService(true);
            grpc::reflection::InitProtoReflectionServerBuilderPlugin();
            grpc::ServerBuilder builder;
            for (auto address : listenAddrs) {
                LOGV2(99999, "listening on address: {address}", "address"_attr = address);
                builder.AddListeningPort(address, grpc::InsecureServerCredentials());
            }

            builder.RegisterService(_service.get());
            _server = builder.BuildAndStart();
            _server->Wait();
        });
    }

    return Status::OK();
}

void TransportLayerGRPC::shutdown() {
    _server->Shutdown();
}

thread_local ASIOReactor* ASIOReactor::_reactorForThread = nullptr;
ReactorHandle TransportLayerGRPC::getReactor(WhichReactor which) {
    // invariant(which == TransportLayer::kNewReactor);
    return std::make_shared<ASIOReactor>();
}

TransportLayerGRPC::GRPCSessionHandle TransportLayerGRPC::getLogicalSessionHandle(
    const std::string& lcid) {
    // NOTE: maybe less lock contention with this approach, but requires double lookup for
    //       session id.
    // absl::flat_hash_map<std::string, GRPCSessionHandle>::iterator sit = _sessions.find(lcid);
    // if (sit == _sessions.end()) {
    //     stdx::lock_guard<Latch> lk(_mutex);
    //     auto [entry, emplaced] =
    //         _sessions.emplace(lcid, std::make_shared<GRPCSession>(this, lcid));
    //     if (!emplaced) {
    //         // throw an exception?
    //     }

    //     _sep->startSession(entry->second);
    //     return entry->second;
    // }
    // return sit->second;

    std::pair<absl::flat_hash_map<std::string, GRPCSessionHandle>::iterator, bool> result;
    stdx::lock_guard<Latch> lk(_mutex);
    result = _sessions.try_emplace(
        lcid, lazy_convert_construct([&] { return std::make_shared<GRPCSession>(this, lcid); }));

    if (result.second) {
        // TODO: how are these ever removed!
        _sep->startSession(result.first->second);
    }

    return result.first->second;
}

}  // namespace transport
}  // namespace mongo
