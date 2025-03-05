#include "cpp/fake_acs_agent_server_reactor.h"

#include <chrono>
#include <memory>
#include <queue>
#include <string>
#include <utility>

#include "absl/functional/any_invocable.h"
#include "absl/log/absl_log.h"
#include "absl/synchronization/mutex.h"
#include "cpp/acs_agent_helper.h"
#include "grpcpp/impl/service_type.h"
#include "grpcpp/security/server_credentials.h"
#include "grpcpp/server_builder.h"
#include "grpcpp/server_context.h"
#include "grpcpp/support/server_callback.h"
#include "grpcpp/support/status.h"

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
  reactor_ = new FakeAcsAgentServerReactor(read_callback_);
  context_ = context;
  for (const auto& [key, value] : initial_metadata_) {
    context_->AddInitialMetadata(key, value);
  }
  return reactor_;
}

void FakeAcsAgentServiceImpl::AddInitialMetadata(const std::string& key,
                                                 const std::string& value) {
  absl::MutexLock lock(&reactor_mtx_);
  if (reactor_ == nullptr) {
    return;
  }
  context_->AddInitialMetadata(key, value);
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
  absl::MutexLock lock(&server_mtx_);
  grpc::ServerBuilder builder;
  server_address_ = "0.0.0.0:50051";
  server_credentials_ = grpc::InsecureServerCredentials();
  builder.AddListeningPort(server_address_, server_credentials_);
  builder.RegisterService(service);
  server_ = builder.BuildAndStart();
}

void FakeAcsAgentServer::Shutdown(
    std::chrono::system_clock::time_point deadline) {
  absl::MutexLock lock(&server_mtx_);
  server_->Shutdown(deadline);
}

void FakeAcsAgentServer::Wait() {
  absl::MutexLock lock(&server_mtx_);
  server_->Wait();
}

std::string FakeAcsAgentServer::GetServerAddress() {
  absl::MutexLock lock(&server_mtx_);
  return server_address_;
}

}  // namespace agent_communication
