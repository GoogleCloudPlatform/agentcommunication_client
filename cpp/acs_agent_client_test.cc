#include "third_party/agentcommunication_client/cpp/acs_agent_client.h"

#include <chrono>
#include <future>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "google/cloud/agentcommunication/v1/agent_communication.grpc.pb.h"
#include "testing/base/public/gmock.h"
#include "testing/base/public/gunit.h"
#include "third_party/absl/base/thread_annotations.h"
#include "third_party/absl/functional/any_invocable.h"
#include "third_party/absl/log/absl_log.h"
#include "third_party/absl/log/globals.h"
#include "third_party/absl/status/status.h"
#include "third_party/absl/status/statusor.h"
#include "third_party/absl/strings/str_cat.h"
#include "third_party/absl/synchronization/mutex.h"
#include "third_party/absl/time/clock.h"
#include "third_party/absl/time/time.h"
#include "third_party/agentcommunication_client/cpp/acs_agent_helper.h"
#include "third_party/agentcommunication_client/cpp/fake_acs_agent_server_reactor.h"
#include "third_party/grpc/include/grpc/grpc.h"
#include "third_party/grpc/include/grpcpp/channel.h"
#include "third_party/grpc/include/grpcpp/create_channel.h"
#include "third_party/grpc/include/grpcpp/security/credentials.h"
#include "third_party/grpc/include/grpcpp/support/channel_arguments.h"
#include "third_party/grpc/include/grpcpp/support/status.h"

namespace agent_communication {
namespace {

// Alias of the stub type used in the ACS Agent Communication service in a .cc
// file.
using AcsStub =
    ::google::cloud::agentcommunication::v1::AgentCommunication::Stub;
using Response =
    ::google::cloud::agentcommunication::v1::StreamAgentMessagesResponse;
using Request =
    ::google::cloud::agentcommunication::v1::StreamAgentMessagesRequest;
using MessageBody = ::google::cloud::agentcommunication::v1::MessageBody;
using agent_communication::AgentConnectionId;

// Custom client channel to hold the response from server.
struct CustomClientChannel {
  absl::Mutex mtx;
  Response response;
  bool response_read = false;
  bool shutdown = false;
};

// Custom server channel to hold the request from client.
struct CustomServerChannel {
  absl::Mutex mtx;
  Request request;
  bool request_read = false;
  bool shutdown = false;
  bool delay_response = false;
  absl::Duration delay_duration = absl::Seconds(3);
};

// Waits for the condition to be true by polling it every sleep_duration till
// timeout.
bool WaitUntil(absl::AnyInvocable<bool()> condition, absl::Duration timeout,
               absl::Duration sleep_duration) {
  absl::Time deadline = absl::Now() + timeout;
  while (absl::Now() < deadline) {
    if (condition()) {
      return true;
    }
    absl::SleepFor(sleep_duration);
  }
  return false;
}

// Test fixture for AcsAgentClient.
// It sets up a fake ACS Agent server and create a client to connect to server.
class AcsAgentClientTest : public ::testing::Test {
 protected:
  AcsAgentClientTest()
      : service_([this](Request request) {
          absl::MutexLock lock(&custom_server_channel_.mtx);
          if (custom_server_channel_.delay_response) {
            absl::SleepFor(custom_server_channel_.delay_duration);
          }
          custom_server_channel_.request = std::move(request);
          custom_server_channel_.request_read = true;
        }),
        server_(&service_) {
    absl::SetGlobalVLogLevel(0);
  }

  void SetUp() override {
    grpc::ChannelArguments channel_args;
    // Keepalive settings
    channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 600 * 1000);  // 600 seconds
    channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS,
                        100 * 1000);  // 100 seconds
    std::shared_ptr<::grpc::Channel> channel = grpc::CreateCustomChannel(
        server_.GetServerAddress(), grpc::InsecureChannelCredentials(),
        channel_args);
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(10);
    ASSERT_TRUE(channel->WaitForConnected(deadline));
    stub_ = google::cloud::agentcommunication::v1::AgentCommunication::NewStub(
        channel);

    // Make sure server does not delay response.
    SetServerDelay(false, absl::ZeroDuration());

    // Create a thread to process the messages from the server. This is to
    // simulate the real use case where the read callback is called in a
    // separate thread. In this test, we just store all the responses in a
    // vector, all_responses_.
    std::promise<bool> read_message_thread_promise;
    std::future<bool> read_message_thread_future =
        read_message_thread_promise.get_future();
    std::thread read_message_thread([this, &read_message_thread_promise]() {
      read_message_thread_promise.set_value(true);
      while (true) {
        custom_client_channel_.mtx.LockWhen(absl::Condition(
            +[](CustomClientChannel *client_channel) {
              return client_channel->response_read || client_channel->shutdown;
            },
            &custom_client_channel_));
        ABSL_VLOG(2) << "response read: "
                     << custom_client_channel_.response_read;
        ABSL_VLOG(2) << "shutdown: " << custom_client_channel_.shutdown;
        if (custom_client_channel_.shutdown) {
          custom_client_channel_.mtx.Unlock();
          break;
        }
        all_responses_.push_back(custom_client_channel_.response);
        custom_client_channel_.response_read = false;
        custom_client_channel_.mtx.Unlock();
      }
    });
    read_message_thread_ = std::move(read_message_thread);
    // Wait for the read message thread to start.
    ASSERT_TRUE(read_message_thread_future.get());

    // Create the client.
    client_ = AcsAgentClient::Create(
        std::move(stub_), AgentConnectionId(), [this](Response response) {
          absl::MutexLock lock(&custom_client_channel_.mtx);
          custom_client_channel_.response = std::move(response);
          ABSL_VLOG(2) << "response read: "
                       << absl::StrCat(custom_client_channel_.response);
          custom_client_channel_.response_read = true;
        });
    ASSERT_OK(client_);

    // Wait for the registration request to be acknowledged by the server.
    EXPECT_TRUE(WaitUntil(
        [this]() {
          absl::MutexLock lock(&custom_client_channel_.mtx);
          return all_responses_.size() == 1;
        },
        absl::Seconds(10), absl::Seconds(1)));
    {
      absl::MutexLock lock(&custom_client_channel_.mtx);
      all_responses_.clear();
    }
  }

  void TearDown() override {
    ABSL_VLOG(2) << "Shutting down fake server during teardown of tests.";
    std::thread wait_for_reactor_termination_([this]() {
      grpc::Status status = (*client_)->AwaitReactor();
      ABSL_VLOG(1) << "reactor terminate status is: " << status.error_code();
    });
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(2);
    server_.GetServer()->Shutdown(deadline);
    server_.GetServer()->Wait();
    wait_for_reactor_termination_.join();
    // Shutting down the read message thread.
    if (read_message_thread_.joinable()) {
      {
        absl::MutexLock lock(&custom_client_channel_.mtx);
        custom_client_channel_.shutdown = true;
      }
      read_message_thread_.join();
    }
  }

  // Sets the server whether to delay the response for the given duration.
  void SetServerDelay(bool delay_response, absl::Duration delay_duration) {
    absl::MutexLock lock(&custom_server_channel_.mtx);
    // Don't wake up the server's callback.
    custom_server_channel_.request_read = false;
    custom_server_channel_.delay_response = delay_response;
    custom_server_channel_.delay_duration = delay_duration;
  }

  std::thread read_message_thread_;
  std::unique_ptr<AcsStub> stub_;
  FakeAcsAgentServiceImpl service_;
  FakeAcsAgentServer server_;
  CustomClientChannel custom_client_channel_;
  CustomServerChannel custom_server_channel_;
  absl::StatusOr<std::unique_ptr<AcsAgentClient>> client_;
  std::vector<Response> ABSL_GUARDED_BY(custom_client_channel_.mtx)
      all_responses_;
};

TEST_F(AcsAgentClientTest, TestSendMessageSucceeds) {
  // Make sure server does not delay response.
  SetServerDelay(false, absl::ZeroDuration());

  // Send a message to the server, expect an OK status.
  MessageBody message_body;
  message_body.mutable_body()->set_value("hello_world");
  ASSERT_OK((*client_)->SendMessage(message_body));

  // Verify that the request is received by the server.
  {
    absl::MutexLock lock(&custom_server_channel_.mtx);
    EXPECT_EQ(custom_server_channel_.request.message_body().body().value(),
              message_body.body().value());
  }
}

TEST_F(AcsAgentClientTest, TestSendMessageTimeout) {
  // Make sure server does delay response.
  SetServerDelay(true, absl::Seconds(3));

  // Send a message to the server, expect timeout status.
  MessageBody message_body;
  message_body.mutable_body()->set_value("hello_world");
  absl::Status send_message_status = (*client_)->SendMessage(message_body);
  ASSERT_EQ(send_message_status.code(), absl::StatusCode::kDeadlineExceeded);

  // We should receive 5 responses from the server because the client will retry
  // to send the request 5 times.
  EXPECT_TRUE(WaitUntil(
      [this]() {
        absl::MutexLock lock(&custom_client_channel_.mtx);
        return all_responses_.size() == 5;
      },
      absl::Seconds(20), absl::Seconds(1)));
}

TEST_F(AcsAgentClientTest, TestReadSuccessfully) {
  // Make sure server does not delay response.
  SetServerDelay(false, absl::ZeroDuration());

  // Create a response with message body and write it to client by server.
  auto response = std::make_unique<Response>();
  response->set_message_id(absl::StrCat(absl::ToUnixMicros(absl::Now())));
  response->mutable_message_body()->mutable_body()->set_value("message_1");

  auto response_copy = std::make_unique<Response>(*response);
  service_.AddResponse(std::move(response));

  // Wait for the response to be read by the client. It should happen instantly.
  ASSERT_TRUE(WaitUntil(
      [this]() {
        absl::MutexLock lock(&custom_client_channel_.mtx);
        return all_responses_.size() == 1;
      },
      absl::Seconds(10), absl::Seconds(1)));
  {
    absl::MutexLock lock(&custom_client_channel_.mtx);
    EXPECT_EQ(all_responses_.size(), 1);
    EXPECT_EQ(all_responses_[0].message_body().body().value(),
              response_copy->message_body().body().value());
  }
}

TEST_F(AcsAgentClientTest, TestReadSuccessfullyAfterWriting) {
  // Make sure server does not delay response.
  SetServerDelay(false, absl::ZeroDuration());

  // Send a message to the server, expect an OK status.
  Request request;
  request.set_message_id(absl::StrCat(absl::ToUnixMicros(absl::Now())));
  request.mutable_message_body()->mutable_body()->set_value("hello_world");
  ASSERT_OK((*client_)->AddRequest(request));
  absl::SleepFor(absl::Milliseconds(100));

  // Create a response with message body and write it to client by server.
  auto response = std::make_unique<Response>();
  response->set_message_id(absl::StrCat(absl::ToUnixMicros(absl::Now())));
  response->mutable_message_body()->mutable_body()->set_value("message_1");
  auto response_copy = std::make_unique<Response>(*response);
  service_.AddResponse(std::move(response));

  // Wait for the response to be read by the client.
  ASSERT_TRUE(WaitUntil(
      [this]() {
        absl::MutexLock lock(&custom_client_channel_.mtx);
        return all_responses_.size() == 2;
      },
      absl::Seconds(10), absl::Seconds(1)));

  {
    absl::MutexLock lock(&custom_client_channel_.mtx);
    EXPECT_EQ(all_responses_.size(), 2);
    EXPECT_EQ(all_responses_[0].message_id(), request.message_id());
    EXPECT_EQ(all_responses_[1].message_body().body().value(),
              response_copy->message_body().body().value());
  }
}

TEST_F(AcsAgentClientTest, TestWriteSuccessfullyAfterReading) {
  // Make sure server does not delay response.
  SetServerDelay(false, absl::ZeroDuration());

  // Create a response with message body and write it to client by server.
  auto response = std::make_unique<Response>();
  response->set_message_id(absl::StrCat(absl::ToUnixMicros(absl::Now())));
  response->mutable_message_body()->mutable_body()->set_value("message_1");
  auto response_copy = std::make_unique<Response>(*response);
  service_.AddResponse(std::move(response));
  absl::SleepFor(absl::Milliseconds(100));

  // Send a message to the server, expect an OK status.
  Request request;
  request.set_message_id(absl::StrCat(absl::ToUnixMicros(absl::Now())));
  request.mutable_message_body()->mutable_body()->set_value("hello_world");
  ASSERT_OK((*client_)->AddRequest(request));

  // Wait for the response to be read by the client.
  ASSERT_TRUE(WaitUntil(
      [this]() {
        absl::MutexLock lock(&custom_client_channel_.mtx);
        return all_responses_.size() == 2;
      },
      absl::Seconds(20), absl::Seconds(1)));

  {
    absl::MutexLock lock(&custom_client_channel_.mtx);
    EXPECT_EQ(all_responses_.size(), 2);
    // The order of the responses is not guaranteed.
    EXPECT_TRUE(all_responses_[0].message_id() == response_copy->message_id() ||
                all_responses_[1].message_id() == response_copy->message_id());
    EXPECT_TRUE(all_responses_[0].message_body().body().value() ==
                    response_copy->message_body().body().value() ||
                all_responses_[1].message_body().body().value() ==
                    response_copy->message_body().body().value());
  }
}

}  // namespace
}  // namespace agent_communication
