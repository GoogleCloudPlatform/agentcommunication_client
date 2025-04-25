#include "cpp/acs_agent_client_reactor.h"

#include <unistd.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "proto/agent_communication.grpc.pb.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/absl_log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/mutex.h"
#include "cpp/acs_agent_helper.h"
#include "grpc/grpc.h"
#include "grpcpp/security/credentials.h"
#include "grpcpp/support/channel_arguments.h"
#include "grpcpp/support/status.h"
#include "grpcpp/support/string_ref.h"

namespace agent_communication {

// Aliases of the protobuf message types used in the ACS Agent Communication
// service in a .cc file.
using Response =
    ::google::cloud::agentcommunication::v1::StreamAgentMessagesResponse;
using Request =
    ::google::cloud::agentcommunication::v1::StreamAgentMessagesRequest;

// Alias of the stub type used in the ACS Agent Communication service in a .cc
// file.
using AcsStub =
    ::google::cloud::agentcommunication::v1::AgentCommunication::Stub;

AcsAgentClientReactor::AcsAgentClientReactor(
    std::unique_ptr<AcsStub> stub,
    absl::AnyInvocable<void(Response, RpcStatus)> read_callback)
    : stub_(std::move(stub)), read_callback_(std::move(read_callback)) {
  stub_->async()->StreamAgentMessages(&context_, this);
  StartRead(&response_);
  StartCall();
}

AcsAgentClientReactor::AcsAgentClientReactor(
    std::unique_ptr<AcsStub> stub,
    absl::AnyInvocable<void(Response, RpcStatus)> read_callback,
    const AgentConnectionId& agent_connection_id)
    : stub_(std::move(stub)), read_callback_(std::move(read_callback)) {
  context_.AddMetadata("authentication", "Bearer " + agent_connection_id.token);
  context_.AddMetadata("agent-communication-resource-id",
                       agent_connection_id.resource_id);
  context_.AddMetadata("agent-communication-channel-id",
                       agent_connection_id.channel_id);
  stub_->async()->StreamAgentMessages(&context_, this);
  StartRead(&response_);
  StartCall();
}

std::unique_ptr<AcsStub> AcsAgentClientReactor::CreateStub(
    const std::string& endpoint) {
  grpc::SslCredentialsOptions options;
  grpc::ChannelArguments channel_args;
  // Keepalive settings
  channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 60 * 1000);  // 60 seconds
  channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS,
                      10 * 1000);  // 10 seconds
  return google::cloud::agentcommunication::v1::AgentCommunication::NewStub(
      grpc::CreateCustomChannel(
          endpoint,
          grpc::SslCredentials(options), channel_args));
}

absl::StatusOr<uint64_t> AcsAgentClientReactor::GetMessagesPerMinuteQuota() {
  absl::MutexLock lock(&status_mtx_);
  return messages_per_minute_quota_;
}

absl::StatusOr<uint64_t> AcsAgentClientReactor::GetBytesPerMinuteQuota() {
  absl::MutexLock lock(&status_mtx_);
  return bytes_per_minute_quota_;
}

bool AcsAgentClientReactor::Cancel() {
  absl::MutexLock lock(&status_mtx_);
  if (rpc_done_) {
    ABSL_VLOG(1)
        << "The RPC has already been terminated when attempting to cancel.";
    return false;
  }
  rpc_cancelled_by_client_ = true;
  context_.TryCancel();
  return true;
}

AcsAgentClientReactor::~AcsAgentClientReactor() {
  Cancel();
  grpc::Status status = Await();
  ABSL_VLOG(1) << absl::StrFormat(
      "AcsAgentClientReactor is destroyed with termination status code: %d "
      "and message: %s and details: %s",
      status.error_code(), status.error_message(), status.error_details());
}

void AcsAgentClientReactor::OnWriteDone(bool ok) {
  if (!ok) {
    ABSL_VLOG(1) << "OnWriteDone not ok";
    return;
  }
  absl::MutexLock lock(&request_mtx_);
  writing_ = false;
  if (request_->has_message_response()) {
    // Pop the queue of ack_buffer_ if the last write was an ack.
    ABSL_VLOG(1) << "Successfully write on the stream with ack with id: "
                 << request_->message_id();
    ack_buffer_.pop();
  } else {
    // Clear up the msg_request_ if the last write was a message.
    ABSL_VLOG(1) << "Successfully write on the stream with message with id: "
                 << request_->message_id();
    msg_request_ = nullptr;
  }
  NextWrite();
}

void AcsAgentClientReactor::Ack(std::string message_id) {
  absl::MutexLock lock(&request_mtx_);
  std::unique_ptr<Request> request = MakeAck(std::move(message_id));
  ack_buffer_.push(std::move(request));
  if (!writing_) {
    NextWrite();
  }
}

void AcsAgentClientReactor::OnReadDone(bool ok) {
  if (!ok) {
    ABSL_VLOG(1) << "OnReadDone not ok";
    {
      absl::MutexLock lock(&status_mtx_);
      if (rpc_cancelled_by_client_) {
        read_callback_(Response(), RpcStatus::kRpcClosedByClient);
        return;
      }
    }
    read_callback_(Response(), RpcStatus::kRpcClosedByServer);
    return;
  }
  if (response_.has_message_body()) {
    ABSL_VLOG(1) << "Client Ack on message with id: " << response_.message_id();
    Ack(response_.message_id());
  }
  read_callback_(std::move(response_), RpcStatus::kRpcOk);
  StartRead(&response_);
}

void AcsAgentClientReactor::OnReadInitialMetadataDone(bool ok) {
  if (!ok) {
    ABSL_LOG(WARNING) << "OnReadInitialMetadataDone not ok";
    return;
  }
  const std::multimap<grpc::string_ref, grpc::string_ref>& metadata =
      context_.GetServerInitialMetadata();
  absl::MutexLock lock(&status_mtx_);
  messages_per_minute_quota_ = GetIntValueFromInitialMetadata<uint64_t>(
      metadata, kMessagesPerMinuteQuotaKey);
  bytes_per_minute_quota_ = GetIntValueFromInitialMetadata<uint64_t>(
      metadata, kBytesPerMinuteQuotaKey);
}

template <typename T>
absl::StatusOr<T> AcsAgentClientReactor::GetIntValueFromInitialMetadata(
    const std::multimap<grpc::string_ref, grpc::string_ref>& metadata,
    const std::string& key) {
  if (metadata.count(key) == 0) {
    return absl::NotFoundError(absl::StrCat(
        "The key: ", key, " was not found in initial metadata from server."));
  }
  auto [range_begin, range_end] = metadata.equal_range(key);
  // Theoretically, there should be <=1 value for the quota keys in the initial
  // metadata of server. If there are multiple values, we will return the first
  // valid value.
  for (auto it = range_begin; it != range_end; ++it) {
    T value = 0;
    std::string value_str(it->second.data(), it->second.size());
    if (!absl::SimpleAtoi(value_str, &value)) {
      ABSL_LOG(WARNING) << "key: " << key
                        << " found in initial metadata from server but its "
                           "value having the wrong format: "
                        << value_str;
    } else {
      return value;
    }
  }
  return absl::NotFoundError(
      absl::StrCat("key: ", key,
                   " was found in initial metadata from server but its value "
                   "was not a valid integer."));
}

void AcsAgentClientReactor::OnDone(const ::grpc::Status& status) {
  absl::MutexLock lock(&status_mtx_);
  ABSL_VLOG(1) << absl::StrFormat(
      "RPC terminated with status code: %d and message: %s and details: %s",
      status.error_code(), status.error_message(), status.error_details());
  rpc_final_status_ = status;
  rpc_done_ = true;
}

grpc::Status AcsAgentClientReactor::Await() {
  status_mtx_.LockWhen(
      absl::Condition(+[](bool* done) { return *done; }, &rpc_done_));
  grpc::Status status = rpc_final_status_;
  status_mtx_.Unlock();
  return status;
}

bool AcsAgentClientReactor::AddRequest(const Request& request) {
  absl::MutexLock lock(&request_mtx_);
  if (msg_request_ == nullptr) {
    // Add the new request to the buffer of reactor, as the last msg_request_
    // was completed.
    msg_request_ = std::make_unique<Request>(request);
    if (!writing_) {
      NextWrite();
    }
    return true;
  } else {
    // Return false as the last msg_request_ was not completed.
    ABSL_VLOG(1) << absl::StrFormat(
        "Failed to add request of id: %s to the buffer of reactor. The last "
        "request of id: %s is not written to the stream yet.",
        msg_request_->message_id(), msg_request_->message_id());
    return false;
  }
}

void AcsAgentClientReactor::NextWrite() {
  if (msg_request_ == nullptr && ack_buffer_.empty()) {
    return;
  }
  writing_ = true;
  if (!ack_buffer_.empty()) {
    // Prioritize the send of ack over message.
    request_ = ack_buffer_.front().get();
  } else {
    request_ = msg_request_.get();
  }
  StartWrite(request_);
}

}  // namespace agent_communication
