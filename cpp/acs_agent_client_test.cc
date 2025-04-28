#include "cpp/acs_agent_client.h"

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "proto/agent_communication.grpc.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/absl_log.h"
#include "absl/log/globals.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "cpp/acs_agent_helper.h"
#include "cpp/fake_acs_agent_server_reactor.h"
#include "grpc/grpc.h"
#include "grpcpp/channel.h"
#include "grpcpp/create_channel.h"
#include "grpcpp/security/credentials.h"
#include "grpcpp/support/channel_arguments.h"

// In external googletest, ASSERT_OK is not defined.
#ifndef ASSERT_OK
#define ASSERT_OK(x) ASSERT_TRUE(x.ok());
#endif
#ifndef ASSERT_OK_AND_ASSIGN
#define ASSERT_OK_AND_ASSIGN(lhs, rexpr) \
  auto statusor = (rexpr);               \
  ASSERT_OK(statusor);                   \
  lhs = std::move(*statusor);
#endif

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
  std::vector<Response> responses ABSL_GUARDED_BY(mtx);
};

// Custom server channel to hold the request from client. Also used to control
// the server behavior on whether to delay the response and for how long.
struct CustomServerChannel {
  absl::Mutex mtx;
  std::vector<Request> requests ABSL_GUARDED_BY(mtx);
  bool delay_response ABSL_GUARDED_BY(mtx) = false;
  absl::Duration delay_duration ABSL_GUARDED_BY(mtx) = absl::Seconds(3);
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

// The condition is expected to be true always in the duration.
bool RemainTrue(absl::AnyInvocable<bool()> condition, absl::Duration timeout,
                absl::Duration sleep_duration) {
  absl::Time deadline = absl::Now() + timeout;
  while (absl::Now() < deadline) {
    if (!condition()) {
      return false;
    }
    absl::SleepFor(sleep_duration);
  }
  return true;
}

// Creates a stub to the ACS Agent Communication service.
std::unique_ptr<AcsStub> CreateStub(std::string address) {
  grpc::ChannelArguments channel_args;
  // Keepalive settings
  channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 600 * 1000);  // 600 seconds
  channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS,
                      100 * 1000);  // 100 seconds
  std::shared_ptr<grpc::Channel> channel = grpc::CreateCustomChannel(
      address, grpc::InsecureChannelCredentials(), channel_args);
  std::chrono::system_clock::time_point deadline =
      std::chrono::system_clock::now() + std::chrono::seconds(20);
  if (!channel->WaitForConnected(deadline)) {
    ABSL_LOG(WARNING) << "Failed to connect to server.";
  }
  return google::cloud::agentcommunication::v1::AgentCommunication::NewStub(
      channel);
}

// Test fixture for AcsAgentClient.
// It sets up a fake ACS Agent server and create a client to connect to server.
class AcsAgentClientTest : public ::testing::Test {
 protected:
  AcsAgentClientTest()
      : service_(
            [this](Request request) {
              // Callback to be invoked in OnReadDone() of the server reactor.
              absl::MutexLock lock(&custom_server_channel_.mtx);
              custom_server_channel_.requests.push_back(std::move(request));
              if (custom_server_channel_.delay_response) {
                absl::SleepFor(custom_server_channel_.delay_duration);
              }
            },
            {{"agent-communication-message-rate-limit", "1"},
             {"agent-communication-bandwidth-limit", "100"}}),
        server_(std::make_unique<FakeAcsAgentServer>(&service_)) {
    absl::SetGlobalVLogLevel(0);
  }

  void SetUp() override {
    stub_ = CreateStub(server_->GetServerAddress());
    // Make sure server does not delay response.
    SetServerDelay(false, absl::ZeroDuration());

    // Create the client. Upon receipt of the Response from server, the client
    // will write the response to custom_client_channel_.
    client_ = AcsAgentClient::Create(
        std::move(stub_), AgentConnectionId(),
        [this](Response response) {
          absl::MutexLock lock(&custom_client_channel_.mtx);
          custom_client_channel_.responses.push_back(std::move(response));
          ABSL_VLOG(2) << "response read: "
                       << absl::StrCat(custom_client_channel_.responses.back());
        },
        [address = server_->GetServerAddress()]() {
          return CreateStub(address);
        },
        []() { return AgentConnectionId(); });
    ASSERT_OK(client_);

    // Wait for the registration request to be acknowledged by the server, and
    // then clear the responses.
    ASSERT_TRUE(WaitUntil(
        [this]() {
          absl::MutexLock lock(&custom_client_channel_.mtx);
          return custom_client_channel_.responses.size() == 1;
        },
        absl::Seconds(10), absl::Seconds(1)));
    {
      absl::MutexLock lock(&custom_client_channel_.mtx);
      custom_client_channel_.responses.clear();
    }

    // Wait for the registration request to be received by the server, and then
    // clear the requests.
    ASSERT_TRUE(WaitUntil(
        [this]() {
          absl::MutexLock lock(&custom_server_channel_.mtx);
          return custom_server_channel_.requests.size() == 1;
        },
        absl::Seconds(10), absl::Seconds(1)));
    {
      absl::MutexLock lock(&custom_server_channel_.mtx);
      custom_server_channel_.requests.clear();
    }
  }

  void TearDown() override {
    ABSL_VLOG(2) << "Shutting down fake server during teardown of tests.";
    if (client_.ok() && *client_ != nullptr) {
      *client_ = nullptr;
    }
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(2);
    server_->Shutdown(deadline);
    server_->Wait();
    server_ = nullptr;
  }

  // Sets the server whether to delay the response for the given duration.
  void SetServerDelay(bool delay_response, absl::Duration delay_duration) {
    absl::MutexLock lock(&custom_server_channel_.mtx);
    custom_server_channel_.delay_response = delay_response;
    custom_server_channel_.delay_duration = delay_duration;
  }

  std::thread read_message_thread_;
  std::unique_ptr<AcsStub> stub_;
  FakeAcsAgentServiceImpl service_;
  std::unique_ptr<FakeAcsAgentServer> server_;
  CustomClientChannel custom_client_channel_;
  CustomServerChannel custom_server_channel_;
  absl::StatusOr<std::unique_ptr<AcsAgentClient>> client_;
};

TEST_F(AcsAgentClientTest, TestClientSendMessagesRepeatedlySuccessful) {
  // Make sure server does not delay response.
  SetServerDelay(false, absl::ZeroDuration());

  // Verify that the client can get the message rate limit and bandwidth limit
  // from the server after the registration request is acknowledged by the
  // server.
  EXPECT_THAT((*client_)->GetMessagePerMinuteQuota(),
              absl_testing::IsOkAndHolds(1));
  EXPECT_THAT((*client_)->GetBytesPerMinuteQuota(),
              absl_testing::IsOkAndHolds(100));

  // Send 50 messages to the server, expect an OK status.
  for (int i = 0; i < 50; ++i) {
    MessageBody message_body;
    message_body.mutable_body()->set_value(absl::StrCat("message_", i));
    ASSERT_OK((*client_)->SendMessage(std::move(message_body)));
  }

  // Wait for the response to be read by the client. It should happen instantly.
  ASSERT_TRUE(WaitUntil(
      [this]() {
        absl::MutexLock lock(&custom_client_channel_.mtx);
        return custom_client_channel_.responses.size() == 50;
      },
      absl::Seconds(10), absl::Seconds(1)));
  // Wait for the acks to be read by the server. It should happen instantly.
  ASSERT_TRUE(WaitUntil(
      [this]() {
        absl::MutexLock lock(&custom_server_channel_.mtx);
        return custom_server_channel_.requests.size() == 50;
      },
      absl::Seconds(10), absl::Seconds(1)));

  // Verify that all acks are received by the client. They should be
  // delivered in order and have the same message id as the requests received by
  // the server. And verify all requests are received by the server as well.
  {
    absl::MutexLock lock1(&custom_client_channel_.mtx);
    absl::MutexLock lock2(&custom_server_channel_.mtx);
    for (int i = 0; i < 50; ++i) {
      EXPECT_TRUE(custom_client_channel_.responses[i].has_message_response());
      EXPECT_EQ(custom_client_channel_.responses[i]
                    .message_response()
                    .status()
                    .code(),
                0);
      EXPECT_EQ(custom_client_channel_.responses[i].message_id(),
                custom_server_channel_.requests[i].message_id());
      EXPECT_EQ(
          custom_server_channel_.requests[i].message_body().body().value(),
          absl::StrCat("message_", i));
    }
    custom_client_channel_.responses.clear();
    custom_server_channel_.requests.clear();
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

  // Wait for the responses to be read by the client.
  ASSERT_TRUE(WaitUntil(
      [this]() {
        absl::MutexLock lock(&custom_client_channel_.mtx);
        return custom_client_channel_.responses.size() == 1;
      },
      absl::Seconds(10), absl::Seconds(1)));

  // Verify that client receive 1 response from the server and verify that
  // server receives 1 request from the client with the same content because
  // the client will not retry.
  {
    absl::MutexLock lock1(&custom_client_channel_.mtx);
    absl::MutexLock lock2(&custom_server_channel_.mtx);
    ASSERT_EQ(custom_server_channel_.requests.size(), 1);
      EXPECT_EQ(custom_client_channel_.responses[0].message_id(),
                custom_server_channel_.requests[0].message_id());
      EXPECT_EQ(custom_client_channel_.responses[0]
                    .message_response()
                    .status()
                    .code(),
                0);
      EXPECT_EQ(
          custom_server_channel_.requests[0].message_body().body().value(),
          "hello_world");
    custom_client_channel_.responses.clear();
    custom_server_channel_.requests.clear();
  }
}

TEST_F(AcsAgentClientTest, TestSendMessageFailsAfterConfiguredTimeout) {
  // Create a new client with a configured timeout of 1 second.
  std::unique_ptr<AcsStub> stub = CreateStub(server_->GetServerAddress());
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<AcsAgentClient> client,
      AcsAgentClient::Create(
          std::move(stub), AgentConnectionId(),
          /*read_callback=*/[](Response) {},
          /*stub_generator=*/
          [address = server_->GetServerAddress()]() {
            return CreateStub(address);
          },
          /*connection_id_generator=*/[]() { return AgentConnectionId(); },
          /*max_wait_time_for_ack=*/std::chrono::seconds(1)));

  // Make sure the server delays the response for more than the configured
  // timeout.
  SetServerDelay(true, absl::Seconds(2));

  // Send a message to the server, expect timeout status.
  MessageBody message_body;
  message_body.mutable_body()->set_value("hello_world");
  absl::Status send_message_status = client->SendMessage(message_body);
  EXPECT_EQ(send_message_status.code(), absl::StatusCode::kDeadlineExceeded);
}

TEST_F(AcsAgentClientTest, TestCreatingClientFailsAfterConfiguredTimeout) {
  // Make sure the server delays the response for more than the configured
  // timeout.
  SetServerDelay(true, absl::Seconds(2));

  // Create a new client with a configured timeout of 1 second.
  std::unique_ptr<AcsStub> stub = CreateStub(server_->GetServerAddress());
  EXPECT_THAT(
      AcsAgentClient::Create(
          std::move(stub), AgentConnectionId(),
          /*read_callback=*/[](Response) {},
          /*stub_generator=*/
          [address = server_->GetServerAddress()]() {
            return CreateStub(address);
          },
          /*connection_id_generator=*/[]() { return AgentConnectionId(); },
          /*max_wait_time_for_ack=*/std::chrono::seconds(1)),
      absl_testing::StatusIs(absl::StatusCode::kDeadlineExceeded));
}

TEST_F(AcsAgentClientTest, TestClientReadMessagesRepeatedlySuccessful) {
  // Make sure server does not delay response.
  SetServerDelay(false, absl::ZeroDuration());

  // Server sends 50 messages to the client.
  std::vector<std::string> message_ids(50);
  for (int i = 0; i < 50; ++i) {
    // Create a response with message body and write it to client by server.
    auto response = std::make_unique<Response>();
    response->set_message_id(absl::StrCat(absl::ToUnixMicros(absl::Now())));
    message_ids[i] = response->message_id();
    response->mutable_message_body()->mutable_body()->set_value(
        absl::StrCat("message_", i));
    auto response_copy = std::make_unique<Response>(*response);
    service_.AddResponse(std::move(response));
  }

  // Wait for the response to be read by the client. It should happen instantly.
  ASSERT_TRUE(WaitUntil(
      [this]() {
        absl::MutexLock lock(&custom_client_channel_.mtx);
        return custom_client_channel_.responses.size() == 50;
      },
      absl::Seconds(10), absl::Seconds(1)));
  // Wait for the acks to be read by the server. It should happen instantly.
  ASSERT_TRUE(WaitUntil(
      [this]() {
        absl::MutexLock lock(&custom_server_channel_.mtx);
        return custom_server_channel_.requests.size() == 50;
      },
      absl::Seconds(10), absl::Seconds(1)));

  // Verify that client receives all 50 messages with right content and server
  // has received 50 acks with right message ids.
  {
    absl::MutexLock lock1(&custom_client_channel_.mtx);
    absl::MutexLock lock2(&custom_server_channel_.mtx);
    for (int i = 0; i < 50; ++i) {
      EXPECT_EQ(custom_client_channel_.responses[i].message_id(),
                message_ids[i]);
      EXPECT_EQ(
          custom_client_channel_.responses[i].message_body().body().value(),
          absl::StrCat("message_", i));
      EXPECT_TRUE(custom_server_channel_.requests[i].has_message_response());
      EXPECT_EQ(
          custom_server_channel_.requests[i].message_response().status().code(),
          0);
      EXPECT_EQ(custom_server_channel_.requests[i].message_id(),
                message_ids[i]);
    }
    custom_client_channel_.responses.clear();
    custom_server_channel_.requests.clear();
  }
}

TEST_F(AcsAgentClientTest, TestReadSuccessfullyAfterWritingRepeatedly) {
  // Make sure server does not delay response.
  SetServerDelay(false, absl::ZeroDuration());

  // Client and Server send a message to each other, in this order, repeat 50
  // times.
  std::vector<std::string> message_ids_sent_by_server(50);
  for (int i = 0; i < 50; ++i) {
    // Client sends a request to the server, expect an OK status.
    Request request;
    request.set_message_id(absl::StrCat(absl::ToUnixMicros(absl::Now())));
    request.mutable_message_body()->mutable_body()->set_value(
        absl::StrCat("hello_world_", i));
    ASSERT_OK((*client_)->AddRequest(request));

    // Server sends a response to the client.
    auto response = std::make_unique<Response>();
    response->set_message_id(absl::StrCat(absl::ToUnixMicros(absl::Now())));
    message_ids_sent_by_server[i] = response->message_id();
    response->mutable_message_body()->mutable_body()->set_value(
        absl::StrCat("message_", i));
    auto response_copy = std::make_unique<Response>(*response);
    service_.AddResponse(std::move(response));
  }

  // Wait for the response to be read by the client.
  ASSERT_TRUE(WaitUntil(
      [this]() {
        absl::MutexLock lock(&custom_client_channel_.mtx);
        return custom_client_channel_.responses.size() == 100;
      },
      absl::Seconds(10), absl::Seconds(1)));
  // Wait for the request to be read by the server.
  ASSERT_TRUE(WaitUntil(
      [this]() {
        absl::MutexLock lock(&custom_server_channel_.mtx);
        return custom_server_channel_.requests.size() == 100;
      },
      absl::Seconds(10), absl::Seconds(1)));

  // Verify that server has received 50 acks and 50 message bodies. The
  // message id of acks should match message_ids of responses sent by the
  // server. They may not be right next to each other, but they should be in
  // order. Also record the message ids sent by the client.
  std::vector<std::string> message_ids_sent_by_client(50);
  {
    absl::MutexLock lock(&custom_server_channel_.mtx);
    int ack_count = 0;
    int message_body_count = 0;
    for (const Request& request : custom_server_channel_.requests) {
      if (request.has_message_response()) {
        EXPECT_EQ(request.message_response().status().code(), 0);
        EXPECT_EQ(request.message_id(),
                  message_ids_sent_by_server[ack_count++]);
      }
      if (request.has_message_body()) {
        message_ids_sent_by_client[message_body_count] = request.message_id();
        EXPECT_EQ(request.message_body().body().value(),
                  absl::StrCat("hello_world_", message_body_count));
        message_body_count++;
      }
    }
    EXPECT_EQ(ack_count, 50);
    EXPECT_EQ(message_body_count, 50);
    custom_server_channel_.requests.clear();
  }

  // Verify that client has received 50 acks and 50 message bodies in the right
  // order.
  {
    absl::MutexLock lock(&custom_client_channel_.mtx);
    int ack_count = 0;
    int message_body_count = 0;
    for (const Response& response : custom_client_channel_.responses) {
      if (response.has_message_response()) {
        EXPECT_EQ(response.message_response().status().code(), 0);
        EXPECT_EQ(response.message_id(),
                  message_ids_sent_by_client[ack_count++]);
      }
      if (response.has_message_body()) {
        EXPECT_EQ(response.message_id(),
                  message_ids_sent_by_server[message_body_count]);
        EXPECT_EQ(response.message_body().body().value(),
                  absl::StrCat("message_", message_body_count));
        message_body_count++;
      }
    }
    EXPECT_EQ(ack_count, 50);
    EXPECT_EQ(message_body_count, 50);
    custom_client_channel_.responses.clear();
  }
}

TEST_F(AcsAgentClientTest, TestWriteSuccessfullyAfterReadingRepeatedly) {
  // Make sure server does not delay response.
  SetServerDelay(false, absl::ZeroDuration());

  // Server and Client send a message to each other, in this order, repeat 50
  // times.
  std::vector<std::string> message_ids_sent_by_server(50);
  for (int i = 0; i < 50; ++i) {
    // Server sends a response to the client.
    auto response = std::make_unique<Response>();
    response->set_message_id(absl::StrCat(absl::ToUnixMicros(absl::Now())));
    message_ids_sent_by_server[i] = response->message_id();
    response->mutable_message_body()->mutable_body()->set_value(
        absl::StrCat("message_", i));
    auto response_copy = std::make_unique<Response>(*response);
    service_.AddResponse(std::move(response));

    // Client sends a request to the server, expect an OK status.
    Request request;
    request.set_message_id(absl::StrCat(absl::ToUnixMicros(absl::Now())));
    request.mutable_message_body()->mutable_body()->set_value(
        absl::StrCat("hello_world_", i));
    ASSERT_OK((*client_)->AddRequest(request));
  }

  // Wait for the responses to be read by the client.
  ASSERT_TRUE(WaitUntil(
      [this]() {
        absl::MutexLock lock(&custom_client_channel_.mtx);
        return custom_client_channel_.responses.size() == 100;
      },
      absl::Seconds(20), absl::Seconds(1)));
  // Wait for the requests to be read by the server.
  ASSERT_TRUE(WaitUntil(
      [this]() {
        absl::MutexLock lock(&custom_server_channel_.mtx);
        return custom_server_channel_.requests.size() == 100;
      },
      absl::Seconds(20), absl::Seconds(1)));

  // Verify that server has received 50 acks and 50 message bodies.
  std::vector<std::string> message_ids_sent_by_client(50);
  {
    absl::MutexLock lock(&custom_server_channel_.mtx);
    int ack_count = 0;
    int message_body_count = 0;
    for (const Request& request : custom_server_channel_.requests) {
      if (request.has_message_response()) {
        EXPECT_EQ(request.message_response().status().code(), 0);
        EXPECT_EQ(request.message_id(),
                  message_ids_sent_by_server[ack_count++]);
      }
      if (request.has_message_body()) {
        message_ids_sent_by_client[message_body_count] = request.message_id();
        EXPECT_EQ(request.message_body().body().value(),
                  absl::StrCat("hello_world_", message_body_count));
        message_body_count++;
      }
    }
    EXPECT_EQ(ack_count, 50);
    EXPECT_EQ(message_body_count, 50);
    custom_server_channel_.requests.clear();
  }

  // Verify that client has received 50 acks and 50 message bodies.
  {
    absl::MutexLock lock(&custom_client_channel_.mtx);
    int ack_count = 0;
    int message_body_count = 0;
    for (const Response& response : custom_client_channel_.responses) {
      if (response.has_message_response()) {
        EXPECT_EQ(response.message_response().status().code(), 0);
        EXPECT_EQ(response.message_id(),
                  message_ids_sent_by_client[ack_count++]);
      }
      if (response.has_message_body()) {
        EXPECT_EQ(response.message_body().body().value(),
                  absl::StrCat("message_", message_body_count));
        EXPECT_EQ(response.message_id(),
                  message_ids_sent_by_server[message_body_count]);
        message_body_count++;
      }
    }
  }
}

TEST_F(AcsAgentClientTest, TestClientRecreatedAfterServerCancellation) {
  // Make sure server does not delay response.
  SetServerDelay(false, absl::ZeroDuration());
  std::chrono::system_clock::time_point deadline =
      std::chrono::system_clock::now() + std::chrono::seconds(1);
  server_->Shutdown(deadline);
  server_->Wait();
  server_ = nullptr;
  server_ = std::make_unique<FakeAcsAgentServer>(&service_);

  // Create a channel to the server, and ensure server is connectable.
  grpc::ChannelArguments channel_args;
  // Keepalive settings
  channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 600 * 1000);  // 600 seconds
  channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS,
                      100 * 1000);  // 100 seconds
  std::shared_ptr<grpc::Channel> channel = grpc::CreateCustomChannel(
      server_->GetServerAddress(), grpc::InsecureChannelCredentials(),
      channel_args);
  deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
  ASSERT_TRUE(channel->WaitForConnected(deadline));

  Request request;
  request.set_message_id(absl::StrCat(absl::ToUnixMicros(absl::Now())));
  request.mutable_message_body()->mutable_body()->set_value(
      absl::StrCat("hello_world_0"));
  ASSERT_TRUE(WaitUntil(
      [this, &request]() { return (*client_)->AddRequest(request).ok(); },
      absl::Seconds(10), absl::Milliseconds(100)));

  ASSERT_TRUE(WaitUntil(
      [this]() {
        absl::MutexLock lock(&custom_client_channel_.mtx);
        return custom_client_channel_.responses.size() == 2;
      },
      absl::Seconds(10), absl::Seconds(0.1)));

  ASSERT_TRUE(WaitUntil(
      [this]() {
        absl::MutexLock lock(&custom_server_channel_.mtx);
        return custom_server_channel_.requests.size() == 2;
      },
      absl::Seconds(10), absl::Seconds(0.1)));

  // Verify that the server has received two requests, one for registration and
  // one for the request.
  std::string registration_message_id;
  {
    absl::MutexLock lock(&custom_server_channel_.mtx);
    EXPECT_TRUE(custom_server_channel_.requests[0].has_register_connection());
    registration_message_id = custom_server_channel_.requests[0].message_id();
    EXPECT_EQ(custom_server_channel_.requests[1].message_id(),
              request.message_id());
    EXPECT_EQ(custom_server_channel_.requests[1].message_body().body().value(),
              "hello_world_0");
    custom_server_channel_.requests.clear();
  }

  // Verify that client has received 2 responses, one for the registration and
  // one for request.
  {
    absl::MutexLock lock(&custom_client_channel_.mtx);
    EXPECT_EQ(custom_client_channel_.responses[0].message_id(),
              registration_message_id);
    EXPECT_EQ(custom_client_channel_.responses[1].message_id(),
              request.message_id());
    custom_client_channel_.responses.clear();
  }
}

TEST_F(AcsAgentClientTest, TestFailureToRegisterConnection) {
  // Shutdown the client and test if IsClientDead() returns true.
  (*client_)->Shutdown();
  EXPECT_TRUE((*client_)->IsDead());
  *client_ = nullptr;

  // Make sure server does delay response.
  SetServerDelay(true, absl::Seconds(5));
  std::chrono::system_clock::time_point deadline =
      std::chrono::system_clock::now() + std::chrono::seconds(1);
  server_->Shutdown(deadline);
  server_->Wait();
  server_ = nullptr;
  server_ = std::make_unique<FakeAcsAgentServer>(&service_);

  // Create a channel to the server, and ensure server is connectable.
  grpc::ChannelArguments channel_args;
  // Keepalive settings
  channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 600 * 1000);  // 600 seconds
  channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS,
                      100 * 1000);  // 100 seconds
  std::shared_ptr<grpc::Channel> channel = grpc::CreateCustomChannel(
      server_->GetServerAddress(), grpc::InsecureChannelCredentials(),
      channel_args);
  deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
  ASSERT_TRUE(channel->WaitForConnected(deadline));

  client_ = AcsAgentClient::Create(
      CreateStub(server_->GetServerAddress()), AgentConnectionId(),
      [this](Response response) {
        absl::MutexLock lock(&custom_client_channel_.mtx);
        custom_client_channel_.responses.push_back(std::move(response));
        ABSL_VLOG(2) << "response read: "
                     << absl::StrCat(custom_client_channel_.responses.back());
      },
      [address = server_->GetServerAddress()]() { return CreateStub(address); },
      []() { return AgentConnectionId(); });
  EXPECT_EQ(client_.status().code(), absl::StatusCode::kDeadlineExceeded);
  EXPECT_THAT(client_.status().ToString(),
              testing::HasSubstr(
                  "Timeout waiting for promise to be set for message with id"));
}

TEST_F(AcsAgentClientTest, TestFailureToRestartClientAndClientIsDead) {
  // Make sure server does delay response, then registration will fail to
  // complete due to timeout.
  SetServerDelay(true, absl::Seconds(5));
  std::chrono::system_clock::time_point deadline =
      std::chrono::system_clock::now() + std::chrono::seconds(1);
  server_->Shutdown(deadline);
  server_->Wait();
  server_ = nullptr;
  server_ = std::make_unique<FakeAcsAgentServer>(&service_);

  // Create a channel to the server, and ensure server is connectable.
  grpc::ChannelArguments channel_args;
  // Keepalive settings
  channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 600 * 1000);  // 600 seconds
  channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS,
                      100 * 1000);  // 100 seconds
  std::shared_ptr<grpc::Channel> channel = grpc::CreateCustomChannel(
      server_->GetServerAddress(), grpc::InsecureChannelCredentials(),
      channel_args);
  deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
  ASSERT_TRUE(channel->WaitForConnected(deadline));

  // Client will transition to kStreamFailedToInitialize eventually.
  EXPECT_TRUE(WaitUntil([this]() { return (*client_)->IsDead(); },
                        absl::Seconds(20), absl::Seconds(0.1)));

  // client is dead, the quota should not be available.
  EXPECT_THAT((*client_)->GetMessagePerMinuteQuota(),
              absl_testing::StatusIs(absl::StatusCode::kFailedPrecondition,
                                     "stream not initialized."));
  EXPECT_THAT((*client_)->GetBytesPerMinuteQuota(),
              absl_testing::StatusIs(absl::StatusCode::kFailedPrecondition,
                                     "stream not initialized."));
}

TEST_F(AcsAgentClientTest,
       TestClientDoesNotCrashAfterShutdownWithPendingResponseFromServer) {
  // Test that the client does not crash and shutdown cleanly after shutdown
  // with a pending response from the server.
  SetServerDelay(/*delay_response=*/true, /*delay_duration=*/absl::Seconds(2.1));
  service_.AddResponse(std::make_unique<Response>());
  *client_ = nullptr;
}

TEST_F(AcsAgentClientTest,
       TestClientDoesNotRestartAfterClientCancelsTheStream) {
  (*client_)->Shutdown();
  ASSERT_TRUE(RemainTrue(
      [&]() {
        absl::MutexLock lock(&custom_client_channel_.mtx);
        return (*client_)->IsDead() && custom_client_channel_.responses.empty();
      },
      absl::Seconds(3), absl::Seconds(0.1)));
}

}  // namespace
}  // namespace agent_communication
