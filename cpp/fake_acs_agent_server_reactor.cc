#include "third_party/agentcommunication_client/cpp/fake_acs_agent_server_reactor.h"

#include <memory>
#include <queue>
#include <string>
#include <utility>

#include "third_party/absl/functional/any_invocable.h"
#include "third_party/absl/log/absl_log.h"
#include "third_party/absl/synchronization/mutex.h"
#include "third_party/agentcommunication_client/cpp/acs_agent_helper.h"
#include "third_party/grpc/include/grpcpp/impl/service_type.h"
#include "third_party/grpc/include/grpcpp/security/server_credentials.h"
#include "third_party/grpc/include/grpcpp/server_builder.h"
#include "third_party/grpc/include/grpcpp/server_context.h"
#include "third_party/grpc/include/grpcpp/support/server_callback.h"
#include "third_party/grpc/include/grpcpp/support/status.h"

namespace agent_communication {

// Alias of the protobuf message types used in the ACS Agent Communication
// service in a .cc file.
using Response =
    ::google::cloud::agentcommunication::v1::StreamAgentMessagesResponse;
using Request =
    ::google::cloud::agentcommunication::v1::StreamAgentMessagesRequest;

void FakeAcsAgentServerReactor::AddResponse(
    std::unique_ptr<
        google::cloud::agentcommunication::v1::StreamAgentMessagesResponse>
        response) {
  absl::MutexLock lock(&response_mtx_);
  responses_.push(std::move(response));
  if (!writing_) {
    NextWrite();
  }
}

void FakeAcsAgentServerReactor::OnDone() {
  ABSL_LOG(INFO) << "server reactor is done";
  delete this;
}

void FakeAcsAgentServerReactor::OnReadDone(bool ok) {
  if (!ok) {
    Finish(grpc::Status(grpc::StatusCode::UNKNOWN, "Unexpected read failure"));
    return;
  }
  read_callback_(request_);
  if (request_.has_message_body() || request_.has_register_connection()) {
    std::unique_ptr<
        google::cloud::agentcommunication::v1::StreamAgentMessagesResponse>
        ack = MakeAckResponse(request_.message_id());
    absl::MutexLock lock(&response_mtx_);
    responses_.push(std::move(ack));
    if (!writing_) {
      NextWrite();
    }
  }
  StartRead(&request_);
}

void FakeAcsAgentServerReactor::OnWriteDone(bool ok) {
  if (!ok) {
    return;
  }
  absl::MutexLock lock(&response_mtx_);
  writing_ = false;
  if (!responses_.empty()) {
    responses_.pop();
  }
  NextWrite();
}

void FakeAcsAgentServerReactor::NextWrite() {
  if (responses_.empty()) {
    return;
  }
  writing_ = true;
  StartWrite(responses_.front().get());
}

grpc::ServerBidiReactor<Request, Response>*
FakeAcsAgentServiceImpl::StreamAgentMessages(
    grpc::CallbackServerContext* context) {
  absl::MutexLock lock(&reactor_mtx_);
  reactor_ = new FakeAcsAgentServerReactor(std::move(read_callback_));
  return reactor_;
}

void FakeAcsAgentServiceImpl::AddResponse(std::unique_ptr<Response> response) {
  absl::MutexLock lock(&reactor_mtx_);
  if (reactor_ == nullptr) {
    return;
  }
  reactor_->AddResponse(std::move(response));
}

bool FakeAcsAgentServiceImpl::IsReactorCreated() {
  absl::MutexLock lock(&reactor_mtx_);
  return reactor_ != nullptr;
}

FakeAcsAgentServer::FakeAcsAgentServer(grpc::Service* service) {
  grpc::ServerBuilder builder;
  server_address_ = "0.0.0.0:50051";
  builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
  builder.RegisterService(service);
  server_ = builder.BuildAndStart();
}

}  // namespace agent_communication
