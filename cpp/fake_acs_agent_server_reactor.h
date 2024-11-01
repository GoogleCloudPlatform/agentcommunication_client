#include <chrono>
#include <functional>
#include <memory>
#include <queue>
#include <string>
#include <utility>

#include "google/cloud/agentcommunication/v1/agent_communication.grpc.pb.h"
#include "third_party/absl/base/thread_annotations.h"
#include "third_party/absl/functional/any_invocable.h"
#include "third_party/absl/log/absl_log.h"
#include "third_party/absl/synchronization/mutex.h"
#include "third_party/grpc/include/grpcpp/impl/service_type.h"
#include "third_party/grpc/include/grpcpp/security/server_credentials.h"
#include "third_party/grpc/include/grpcpp/server_builder.h"
#include "third_party/grpc/include/grpcpp/server_context.h"
#include "third_party/grpc/include/grpcpp/support/server_callback.h"
#include "third_party/grpc/include/grpcpp/support/status.h"

namespace agent_communication {

// Fake ACS agent server reactor.
// This class is used to create a fake ACS agent server for testing the
// functionality of the client reactor.
class FakeAcsAgentServerReactor
    : public grpc::ServerBidiReactor<
          google::cloud::agentcommunication::v1::StreamAgentMessagesRequest,
          google::cloud::agentcommunication::v1::StreamAgentMessagesResponse> {
 public:
  explicit FakeAcsAgentServerReactor(
      std::function<void(
          google::cloud::agentcommunication::v1::StreamAgentMessagesRequest)>
          read_callback)
      : read_callback_(read_callback) {
    StartRead(&request_);
  }

  // Adds a response to the buffer of the server reactor and triggers a write
  // operation if there is no write operation in flight.
  void AddResponse(
      std::unique_ptr<
          google::cloud::agentcommunication::v1::StreamAgentMessagesResponse>
          response) ABSL_LOCKS_EXCLUDED(response_mtx_);

 private:
  // Override methods of ServerBidiReactor. These methods are called by gRPC
  // framework. OnDone is called when the RPC is terminated. OnCancel is called
  // when the RPC is cancelled by the client. OnReadDone is called when the read
  // operation is done. OnWriteDone is called when the write operation is done.
  void OnDone() override;

  void OnCancel() override { ABSL_LOG(INFO) << "server reactor is cancelled"; }

  void OnReadDone(bool ok) ABSL_LOCKS_EXCLUDED(response_mtx_) override;

  void OnWriteDone(bool ok) ABSL_LOCKS_EXCLUDED(response_mtx_) override;

  // Triggers a StartWrite operation if there is no write operation in flight
  // and there is a non-empty response in the buffer.
  void NextWrite() ABSL_EXCLUSIVE_LOCKS_REQUIRED(response_mtx_);

  // Callback invoked in OnReadDone to process the request from the client.
  // Injected during construction to allow the caller of this class to
  // control how to process the messages from the client.
  std::function<void(
      google::cloud::agentcommunication::v1::StreamAgentMessagesRequest)>
      read_callback_;

  absl::Mutex response_mtx_;

  // Whether there is a write operation in flight.
  bool writing_ ABSL_GUARDED_BY(response_mtx_) = false;

  // Buffer to store the responses to be sent to the client.
  std::queue<std::unique_ptr<
      google::cloud::agentcommunication::v1::StreamAgentMessagesResponse> >
      responses_ ABSL_GUARDED_BY(response_mtx_);

  // Buffer to store the request received from the client.
  google::cloud::agentcommunication::v1::StreamAgentMessagesRequest request_;
};

// Fake ACS agent service implementation.
// This class is used to create a fake ACS agent service for testing the
// functionality of the client reactor.
class FakeAcsAgentServiceImpl final
    : public google::cloud::agentcommunication::v1::AgentCommunication::
          CallbackService {
 public:
  explicit FakeAcsAgentServiceImpl(
      std::function<void(
          google::cloud::agentcommunication::v1::StreamAgentMessagesRequest)>
          read_callback)
      : read_callback_(read_callback) {}

  grpc::ServerBidiReactor<
      google::cloud::agentcommunication::v1::StreamAgentMessagesRequest,
      google::cloud::agentcommunication::v1::StreamAgentMessagesResponse>*
  StreamAgentMessages(grpc::CallbackServerContext* context)
      ABSL_LOCKS_EXCLUDED(reactor_mtx_) override;

  // Adds a response to the buffer of the server reactor.
  void AddResponse(
      std::unique_ptr<
          google::cloud::agentcommunication::v1::StreamAgentMessagesResponse>
          response) ABSL_LOCKS_EXCLUDED(reactor_mtx_);

  bool IsReactorCreated() ABSL_LOCKS_EXCLUDED(reactor_mtx_);

 private:
  absl::Mutex reactor_mtx_;
  grpc::CallbackServerContext* context_ ABSL_GUARDED_BY(reactor_mtx_);
  FakeAcsAgentServerReactor* reactor_ ABSL_GUARDED_BY(reactor_mtx_) = nullptr;
  std::function<void(
      google::cloud::agentcommunication::v1::StreamAgentMessagesRequest)>
      read_callback_;
};

// Fake ACS agent server.
// This class is used to create a fake ACS agent server for testing the
// functionality of the client reactor.
class FakeAcsAgentServer {
 public:
  explicit FakeAcsAgentServer(grpc::Service* service);
  void Shutdown(std::chrono::system_clock::time_point deadline)
      ABSL_LOCKS_EXCLUDED(server_mtx_);
  void Wait() ABSL_LOCKS_EXCLUDED(server_mtx_);
  std::string GetServerAddress() ABSL_LOCKS_EXCLUDED(server_mtx_);

 private:
  absl::Mutex server_mtx_;
  std::unique_ptr<grpc::Server> server_ ABSL_GUARDED_BY(server_mtx_);
  std::string server_address_ ABSL_GUARDED_BY(server_mtx_);
  std::shared_ptr<grpc::ServerCredentials> server_credentials_
      ABSL_GUARDED_BY(server_mtx_);
};

}  // namespace agent_communication
