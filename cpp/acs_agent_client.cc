#include "third_party/agentcommunication_client/cpp/acs_agent_client.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <future>
#include <limits>
#include <memory>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include "google/cloud/agentcommunication/v1/agent_communication.grpc.pb.h"
#include "third_party/absl/functional/any_invocable.h"
#include "third_party/absl/functional/bind_front.h"
#include "third_party/absl/log/absl_log.h"
#include "third_party/absl/memory/memory.h"
#include "third_party/absl/random/distributions.h"
#include "third_party/absl/status/status.h"
#include "third_party/absl/status/statusor.h"
#include "third_party/absl/strings/str_cat.h"
#include "third_party/absl/strings/str_format.h"
#include "third_party/absl/strings/string_view.h"
#include "third_party/absl/synchronization/mutex.h"
#include "third_party/absl/time/clock.h"
#include "third_party/absl/time/time.h"
#include "third_party/agentcommunication_client/cpp/acs_agent_client_reactor.h"
#include "third_party/agentcommunication_client/cpp/acs_agent_helper.h"
#include "third_party/grpc/include/grpcpp/support/status.h"

namespace agent_communication {

// Alias of the protobuf message types used in the ACS Agent Communication
// service in a .cc file.
using Response =
    ::google::cloud::agentcommunication::v1::StreamAgentMessagesResponse;
using Request =
    ::google::cloud::agentcommunication::v1::StreamAgentMessagesRequest;
using MessageBody = ::google::cloud::agentcommunication::v1::MessageBody;
using AcsStub =
    ::google::cloud::agentcommunication::v1::AgentCommunication::Stub;

absl::StatusOr<std::unique_ptr<AcsAgentClient>> AcsAgentClient::Create(
    std::unique_ptr<
        google::cloud::agentcommunication::v1::AgentCommunication::Stub>
        stub,
    AgentConnectionId agent_connection_id,
    absl::AnyInvocable<void(
        google::cloud::agentcommunication::v1::StreamAgentMessagesResponse)>
        read_callback,
    absl::AnyInvocable<std::unique_ptr<AcsStub>()> stub_generator) {
  // Create the client.
  std::unique_ptr<AcsAgentClient> client = absl::WrapUnique(
      new AcsAgentClient(agent_connection_id, std::move(read_callback),
                         std::move(stub_generator)));

  // Start the read message thread.
  std::thread read_message_body_thread(
      absl::bind_front(&AcsAgentClient::ClientReadMessage, client.get()));
  client->read_response_thread_ = std::move(read_message_body_thread);

  // Initialize the client.
  {
    absl::MutexLock lock(&client->reactor_mtx_);
    client->reactor_ = std::make_unique<AcsAgentClientReactor>(
        std::move(stub),
        absl::bind_front(&AcsAgentClient::ReactorReadCallback, client.get()),
        std::move(agent_connection_id));
    absl::Status init_status = client->Init();
    if (!init_status.ok()) {
      return init_status;
    }
  }

  // Start the restart client thread after the Init() was successful.
  std::thread restart_client_thread(
      absl::bind_front(&AcsAgentClient::RestartClient, client.get()));
  client->restart_client_thread_ = std::move(restart_client_thread);
  return client;
}

absl::Status AcsAgentClient::AddRequest(Request& request) {
  absl::Status latest_send_request_status = absl::OkStatus();
  // TODO: Make the retry parameters configurable.
  for (int i = 0; i < 5; ++i) {
    // Generate a new message id for each attempt.
    {
      absl::MutexLock lock(&reactor_mtx_);
      request.set_message_id(CreateMessageUuid());
    }
    latest_send_request_status = AddRequestAndWaitForResponse(request);
    if (latest_send_request_status.ok()) {
      return absl::OkStatus();
    }
    if (latest_send_request_status.code() == absl::StatusCode::kAlreadyExists) {
      // Retry to send the request because the request was not even buffered in
      // the reactor.
      ABSL_VLOG(2) << absl::StrFormat(
          "Failed to add message with id: %s to reactor as the ongoing write "
          "takes. Immediately retrying without sleeping.",
          request.message_id());
      continue;
    }
    if (latest_send_request_status.code() ==
        absl::StatusCode::kDeadlineExceeded) {
      // Retry to send the request because the wait for the response timed out.
      ABSL_LOG(WARNING) << absl::StrFormat(
          "Successfully added message with id: %s to reactor, but timed out "
          "waiting for response from server. Immediately retrying without "
          "sleeping.",
          request.message_id());
      continue;
    }
    if (latest_send_request_status.code() ==
        absl::StatusCode::kResourceExhausted) {
      // Retry to send the request because the resource exhausted with backoff.
      ABSL_LOG(WARNING) << absl::StrFormat(
          "Successfully added message with id: %s to reactor, but get a "
          "resource exhausted error. Retrying with a sleep delay.",
          request.message_id());
      int delayMillis = std::min(100 * (1 << i), 2000);
      absl::SleepFor(absl::Milliseconds(delayMillis));
      continue;
    }
    // Stop retrying if the send failed due to any other error.
    break;
  }
  return latest_send_request_status;
}

absl::Status AcsAgentClient::SendMessage(MessageBody message_body) {
  Request request;
  *request.mutable_message_body() = std::move(message_body);
  return AddRequest(request);
}

absl::Status AcsAgentClient::Init() {
  // Register the connection. The registration request should only be sent once.
  std::unique_ptr<Request> registration_request =
      agent_communication::MakeRequestWithRegistration(
          CreateMessageUuid(), connection_id_.channel_id,
          connection_id_.resource_id);
  if (absl::Status status = RegisterConnection(*registration_request);
      !status.ok()) {
    return status;
  }
  stream_state_ = ClientState::kReady;
  return absl::OkStatus();
}

absl::Status AcsAgentClient::AddRequestAndWaitForResponse(
    const Request& request) {
  // Queue up the message in the reactor and create the promise-future pair to
  // wait for the response from the server.
  bool added_to_reactor = false;
  const std::string& message_id = request.message_id();
  std::promise<absl::Status> responsePromise;
  std::future<absl::Status> responseFuture = responsePromise.get_future();
  {
    absl::MutexLock lock(&request_delivery_status_mtx_);
    attempted_requests_responses_sub_.emplace(message_id,
                                              std::move(responsePromise));
  }
  // TODO: Make the retry parameters configurable.
  for (int i = 0; i < 5; ++i) {
    {
      absl::MutexLock lock(&reactor_mtx_);
      if (request.has_register_connection() &&
          (stream_state_ != ClientState::kStreamNotInitialized &&
           stream_state_ != ClientState::kStreamClosed)) {
        return absl::InternalError(
            "The stream is not in the correct state to accept new registration "
            "request.");
      }
      if (request.has_message_body() && stream_state_ != ClientState::kReady) {
        return absl::FailedPreconditionError(
            "The stream is not ready to accept new MessageBody.");
      }
      if (reactor_->AddRequest(request)) {
        added_to_reactor = true;
        break;
      }
    }
    int delayMillis = std::min(100 * (1 << i), 2000);
    absl::SleepFor(absl::Milliseconds(delayMillis));
  }
  if (!added_to_reactor) {
    // Set a dummy status value and clean up the promise if we fail to add the
    // request to the reactor.
    absl::MutexLock lock(&request_delivery_status_mtx_);
    SetValueAndRemovePromise(message_id, absl::OkStatus());
    ABSL_LOG(WARNING) << absl::StrFormat(
        "Failed to add message with id: %s to reactor as the ongoing write "
        "takes too long.",
        message_id);
    return absl::AlreadyExistsError(
        "Failed to add message to reactor because the ongoing write takes too "
        "long.");
  }

  // Now that we have added the request to the reactor, wait for the response
  // from the server.
  std::future_status status = responseFuture.wait_for(std::chrono::seconds(2));
  absl::Status received_status = absl::OkStatus();
  if (status == std::future_status::ready) {
    received_status = responseFuture.get();
    return received_status;
  }

  if (status == std::future_status::timeout) {
    ABSL_LOG(WARNING) << "timeout of waiting for response: " << message_id;
    received_status = absl::DeadlineExceededError(absl::StrFormat(
        "Timeout waiting for promise to be set for message with id: %s.",
        message_id));
  }
  if (status == std::future_status::deferred) {
    ABSL_LOG(WARNING)
        << "This should never happen: get a deferred status from the future "
           "when waiting for response for message with id: "
        << message_id;
    received_status = absl::InternalError(absl::StrFormat(
        "Future is deferred for message with id: %s. This should never happen.",
        message_id));
  }
  // Set a dummy status value and clean up the promise if we don't receive the
  // response from the server.
  absl::MutexLock lock(&request_delivery_status_mtx_);
  SetValueAndRemovePromise(message_id, absl::OkStatus());
  return received_status;
}

bool AcsAgentClient::ShouldWakeUpClientReadMessage() {
  return !msg_responses_.empty() ||
         client_read_state_ == ClientState::kShutdown;
}

void AcsAgentClient::ClientReadMessage() {
  while (true) {
    // This thread will be woken up by the ReactorReadCallback() when reactor
    // calls OnReadDone() or woken up by Shutdown().
    // Within every iteration, if we don't shutdown, we will pop out 1 message,
    // exit the critical section, and then process the message by calling
    // AckOnSuccessfulDelivery() and read_callback_. In this way, we can release
    // the lock response_read_mtx_ and avoid blocking the OnReadDone() call of
    // the reactor.
    response_read_mtx_.LockWhen(
        absl::Condition(this, &AcsAgentClient::ShouldWakeUpClientReadMessage));
    if (client_read_state_ == ClientState::kShutdown) {
      response_read_mtx_.Unlock();
      return;
    }
    if (msg_responses_.empty()) {
      response_read_mtx_.Unlock();
      continue;
    }
    Response response = std::move(msg_responses_.front());
    msg_responses_.pop();
    response_read_mtx_.Unlock();

    // Exit the critical section and process the message.
    if (response.has_message_response()) {
      AckOnSuccessfulDelivery(response);
    }
    read_callback_(std::move(response));
  }
}

void AcsAgentClient::ReactorReadCallback(Response response, bool ok) {
  if (!ok) {
    ABSL_LOG(WARNING) << "ReactorReadCallback not ok";
    // Wakes up RestartReactor() to restart the stream.
    absl::MutexLock lock(&reactor_mtx_);
    stream_state_ = ClientState::kStreamClosed;
    return;
  }
  // Wake up ClientReadMessage().
  absl::MutexLock lock(&response_read_mtx_);
  msg_responses_.push(std::move(response));
  ABSL_VLOG(2) << "Producer called with response: "
               << absl::StrCat(msg_responses_.front());
}

absl::Status AcsAgentClient::RegisterConnection(const Request& request) {
  // Add request message to the reactor and create the promise-future pair to
  // wait for the response from the server.
  const std::string& message_id = request.message_id();
  std::promise<absl::Status> responsePromise;
  std::future<absl::Status> responseFuture = responsePromise.get_future();
  {
    absl::MutexLock lock(&request_delivery_status_mtx_);
    attempted_requests_responses_sub_.emplace(message_id,
                                              std::move(responsePromise));
  }
  bool added_to_reactor = reactor_->AddRequest(request);
  if (!added_to_reactor) {
    absl::MutexLock lock(&request_delivery_status_mtx_);
    SetValueAndRemovePromise(message_id, absl::OkStatus());
    return absl::InternalError(
        "Failed to add registration request to reactor, because the existing "
        "write buffer is full. This should never happen, because the "
        "registration request should be the first request sent to the server.");
  }

  // Now that we have added the request to the reactor, wait for the response
  // from the server.
  // Note that during the wait here, we hold the reactor_mtx_ lock. This is
  // intentional to keep the client from sending any other requests. The
  // downside is that if reactor calls OnReadDone(ok=false), which indicates the
  // failure of register connection, ReactorReadCallback() will not be able to
  // acquire the reactor_mtx_ lock until the wait here is done. This is fine
  // for now, because this function will still return a failed status, and wait
  // for the caller of this class to retry.
  std::future_status status = responseFuture.wait_for(std::chrono::seconds(2));
  absl::Status received_status = absl::OkStatus();
  if (status == std::future_status::ready) {
    received_status = responseFuture.get();
    return received_status;
  }

  if (status == std::future_status::timeout) {
    ABSL_LOG(WARNING) << "timeout of waiting for response: " << message_id;
    received_status = absl::DeadlineExceededError(absl::StrFormat(
        "Timeout waiting for promise to be set for message with id: %s.",
        message_id));
  }
  if (status == std::future_status::deferred) {
    ABSL_LOG(WARNING)
        << "This should never happen: get a deferred status from the future "
           "when waiting for response for message with id: "
        << message_id;
    received_status = absl::InternalError(absl::StrFormat(
        "Future is deferred for message with id: %s. This should never happen.",
        message_id));
  }

  // Set a dummy status value and clean up the promise if we don't receive the
  // response from the server.
  absl::MutexLock lock(&request_delivery_status_mtx_);
  SetValueAndRemovePromise(message_id, absl::OkStatus());
  return received_status;
}

void AcsAgentClient::RestartClient() {
  while (true) {
    reactor_mtx_.LockWhen(absl::Condition(
        +[](ClientState* stream_state) {
          return *stream_state == ClientState::kStreamClosed ||
                 *stream_state == ClientState::kShutdown;
        },
        &stream_state_));
    // Terminate the thread if the client is being shutdown.
    if (stream_state_ == ClientState::kShutdown) {
      reactor_mtx_.Unlock();
      return;
    }

    // Wait for the reactor to be terminated, capture the status, and then
    // restart the reactor.
    // TODO: need to determine if we want to retry based on the status, and add
    // retry logic with backoff mechanism.
    if (reactor_ != nullptr) {
      grpc::Status status = reactor_->Await();
      ABSL_LOG(INFO) << absl::StrFormat(
          "RestartReactor thread trying to restart the stream with previous "
          "termination status code: %d and message: %s and details: %s",
          status.error_code(), status.error_message(), status.error_details());
    }
    std::unique_ptr<AcsStub> stub = GenerateConnectionIdAndStub();
    if (stub == nullptr) {
      stream_state_ = ClientState::kStreamFailedToRestart;
      reactor_mtx_.Unlock();
      return;
    }
    reactor_ = std::make_unique<AcsAgentClientReactor>(
        std::move(stub),
        absl::bind_front(&AcsAgentClient::ReactorReadCallback, this),
        connection_id_);
    if (reactor_ == nullptr) {
      stream_state_ = ClientState::kStreamFailedToRestart;
      reactor_mtx_.Unlock();
      ABSL_LOG(WARNING) << "Failed to generate connection id and reactor.";
      return;
    }

    // Initialize the client.
    absl::Status init_status = Init();
    if (!init_status.ok()) {
      stream_state_ = ClientState::kStreamFailedToRestart;
      reactor_mtx_.Unlock();
      return;
    }
    reactor_mtx_.Unlock();
  }
}

std::unique_ptr<AcsStub> AcsAgentClient::GenerateConnectionIdAndStub() {
  if (stub_generator_ != nullptr) {
    return stub_generator_();
  }
  absl::StatusOr<AgentConnectionId> new_connection_id =
      agent_communication::GenerateAgentConnectionId(connection_id_.channel_id,
                                                     false);
  if (!new_connection_id.ok()) {
    ABSL_LOG(WARNING) << "Failed to get connection id from agent connection "
                      << "name: " << connection_id_.channel_id;
    return nullptr;
  }
  connection_id_ = *std::move(new_connection_id);
  return AcsAgentClientReactor::CreateStub(connection_id_.channel_id);
}

void AcsAgentClient::AckOnSuccessfulDelivery(const Response& response) {
  absl::MutexLock lock(&request_delivery_status_mtx_);
  const std::string& message_id = response.message_id();
  if (attempted_requests_responses_sub_.find(message_id) ==
      attempted_requests_responses_sub_.end()) {
    ABSL_LOG(WARNING) << absl::StrFormat(
        "Failed to find the promise for message with id: %s, but we got the "
        "response from the server with content: %s",
        message_id, response.DebugString());
    return;
  }
  // Convert the google::rpc::status proto to absl::status object and set the
  // promise.
  absl::StatusCode code = static_cast<absl::StatusCode>(
      response.message_response().status().code());
  SetValueAndRemovePromise(
      message_id,
      absl::Status(code, response.message_response().status().message()));
}

void AcsAgentClient::Shutdown() {
  if (read_response_thread_.joinable()) {
    {
      // Wakes up ClientReadMessage() to shut it down.
      absl::MutexLock lock(&response_read_mtx_);
      client_read_state_ = ClientState::kShutdown;
    }
    read_response_thread_.join();
  }

  // Shutdown the restart_client_thread_ before the RPC is terminated.
  // Otherwise, the restart_client_thread_ may try to restart the RPC.
  if (restart_client_thread_.joinable()) {
    {
      absl::MutexLock lock(&reactor_mtx_);
      stream_state_ = ClientState::kShutdown;
    }
    restart_client_thread_.join();
  }
  {
    // Don't call reactor_Await() here as it will block the thread.
    absl::MutexLock lock(&reactor_mtx_);
    if (reactor_ != nullptr) {
      reactor_->Cancel();
    }
  }
}

std::string AcsAgentClient::CreateMessageUuid() {
  int64_t random =
      absl::Uniform<int64_t>(gen_, 0, std::numeric_limits<int64_t>::max());
  return absl::StrCat(random, "-", absl::ToUnixMicros(absl::Now()));
}

void AcsAgentClient::SetValueAndRemovePromise(const std::string& message_id,
                                              absl::Status status) {
  auto it = attempted_requests_responses_sub_.find(message_id);
  if (it == attempted_requests_responses_sub_.end()) {
    return;
  }
  it->second.set_value(std::move(status));
  attempted_requests_responses_sub_.erase(it);
}

}  // namespace agent_communication
