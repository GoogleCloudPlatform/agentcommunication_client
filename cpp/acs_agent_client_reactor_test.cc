#include "cpp/acs_agent_client_reactor.h"

#include <unistd.h>

#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <utility>

#include "proto/agent_communication.grpc.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/absl_log.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "cpp/fake_acs_agent_server_reactor.h"
#include "grpc/grpc.h"
#include "grpcpp/channel.h"
#include "grpcpp/create_channel.h"
#include "grpcpp/security/credentials.h"
#include "grpcpp/support/channel_arguments.h"
#include "grpcpp/support/status.h"

namespace agent_communication {
namespace {

// Alias of the stub type used in the ACS Agent Communication service in a .cc
// file.
using AcsStub =
    ::google::cloud::agentcommunication::v1::grpc::AgentCommunication::Stub;
// Aliases of the protobuf message types used in the ACS Agent Communication
// service in a .cc file.
using Response =
    ::google::cloud::agentcommunication::v1::StreamAgentMessagesResponse;
using Request =
    ::google::cloud::agentcommunication::v1::StreamAgentMessagesRequest;
using ::testing::AnyOf;

// Waits for the condition to be true for the given timeout.
bool WaitUntil(std::function<bool()> condition, absl::Duration timeout) {
  absl::Time deadline = absl::Now() + timeout;
  while (absl::Now() < deadline) {
    if (condition()) {
      return true;
    }
    absl::SleepFor(absl::Milliseconds(100));
  }
  return false;
}

// Returns a random number between 1 and 1000.
// TODO: use a more robust UUID generator. Currently this is just for testing
// purposes in fake server and the unit tests. We need to put the random number
// generator in the AcsAgentClient class for production non-testing code.
std::string randomNumber() {
  // 1. Create a random number engine (Mersenne Twister in this case)
  std::mt19937 engine;

  // 2. Seed the engine (using std::random_device for non-determinism)
  std::random_device rd;
  engine.seed(rd());

  // 3. Create a distribution (uniform integers between 1 and 100)
  std::uniform_int_distribution<> dist(1, 1000);

  return std::to_string(dist(engine));
}

// Creates a request with the given message as MessageBody, message id as a
// random number between 1 and 1000.
std::unique_ptr<Request> MakeRequestWithBody(std::string message) {
  auto request = std::make_unique<Request>();
  request->set_message_id(randomNumber());
  request->mutable_message_body()->mutable_body()->set_value(
      std::move(message));
  return request;
}

// Creates a response with the given message as MessageBody, message id as a
// random number between 1 and 1000.
std::unique_ptr<Response> MakeResponseWithBody(std::string message) {
  auto response = std::make_unique<Response>();
  response->set_message_id(randomNumber());
  response->mutable_message_body()->mutable_body()->set_value(
      std::move(message));
  return response;
}

// Test fixture for AcsAgentClientReactor.
// Creates a fake ACS agent server and a client reactor to test the
// functionality of the client reactor.
class AcsAgentClientReactorTest : public ::testing::Test {
 protected:
  // Constructor of the test fixture. Set the service's read callback to be
  // request_promise_.set_value() so that the request can be captured in the
  // tests.
  AcsAgentClientReactorTest()
      : service_([this](Request request) {
          request_promise_.set_value(std::move(request));
        }),
        server_(&service_) {}

 protected:
  void SetUp() override {
    ABSL_VLOG(2) << "Setting up";
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
    stub_ = google::cloud::agentcommunication::v1::grpc::AgentCommunication::
        NewStub(channel);
  }

  void TearDown() override {
    ABSL_VLOG(2) << "Shutting down fake server during teardown of tests.";
    // Shutdown the server.
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(2);
    server_.Shutdown(deadline);
    server_.Wait();
  }

  // Promise to capture the request sent by the client.
  std::promise<Request> request_promise_;
  std::future<Request> request_future_ = request_promise_.get_future();
  FakeAcsAgentServiceImpl service_;
  FakeAcsAgentServer server_;
  std::unique_ptr<AcsAgentClientReactor> reactor_;
  std::unique_ptr<AcsStub> stub_;
};

// Tests the functionality of writing a request by the client reactor.
TEST_F(AcsAgentClientReactorTest, TestAddRequest) {
  // Create a client reactor with a read callback to capture the response from
  // the server.
  std::promise<Response> response_promise;
  std::future<Response> response_future = response_promise.get_future();
  reactor_ = std::make_unique<AcsAgentClientReactor>(
      std::move(stub_),
      [&response_promise](Response response,
                          AcsAgentClientReactor::RpcStatus status) {
        if (status == AcsAgentClientReactor::RpcStatus::kRpcOk) {
          response_promise.set_value(std::move(response));
        }
      });
  ASSERT_TRUE(WaitUntil([this]() { return service_.IsReactorCreated(); },
                        absl::Seconds(10)));
  ABSL_LOG(INFO) << "Setup done";

  // Create a request.
  std::unique_ptr<Request> request = MakeRequestWithBody("body test");

  // Add the request to the reactor.
  ASSERT_TRUE(reactor_->AddRequest(*request));
  // Verify that the response is sent to the reactor by the server.
  std::future_status status =
      response_future.wait_for(std::chrono::seconds(10));
  ASSERT_EQ(status, std::future_status::ready);
  Response response = response_future.get();
  EXPECT_EQ(request->message_id(), response.message_id());
  EXPECT_EQ(response.message_response().status().code(), 0);

  // Verify that the request is sent to the server.
  std::future_status request_status =
      request_future_.wait_for(std::chrono::seconds(10));
  ASSERT_EQ(request_status, std::future_status::ready);
  Request request_store = request_future_.get();
  EXPECT_EQ(request_store.message_id(), request->message_id());
  EXPECT_EQ(request_store.message_body().body().value(),
            request->message_body().body().value());
}

// Tests the functionality of OnReadDone() of client when server sends a
// response with message body. The client should send an ack back to the server.
TEST_F(AcsAgentClientReactorTest, TestOnReadSuccessfully) {
  // Create a client reactor with a read callback to capture the response from
  // the server.
  std::promise<Response> response_promise;
  std::future<Response> response_future = response_promise.get_future();
  reactor_ = std::make_unique<AcsAgentClientReactor>(
      std::move(stub_),
      [&response_promise](Response response,
                          AcsAgentClientReactor::RpcStatus status) {
        if (status == AcsAgentClientReactor::RpcStatus::kRpcOk) {
          response_promise.set_value(std::move(response));
        }
      });
  ASSERT_TRUE(WaitUntil([this]() { return service_.IsReactorCreated(); },
                        absl::Seconds(1)));
  ABSL_LOG(INFO) << "Setup done";

  // Create a response with message body and write it to client by server.
  std::unique_ptr<Response> response =
      agent_communication::MakeResponseWithBody("message_1");
  auto response_copy = std::make_unique<Response>(*response);
  service_.AddResponse(std::move(response));

  // Verify that the response is received by the client reactor.
  std::future_status response_status =
      response_future.wait_for(std::chrono::seconds(10));
  Response response_store = response_future.get();
  ASSERT_EQ(response_status, std::future_status::ready);
  EXPECT_EQ(response_store.message_id(), response_copy->message_id());
  EXPECT_EQ(response_store.message_body().body().value(),
            response_copy->message_body().body().value());

  // Verify that the server receives an ack by the client.
  std::future_status request_status =
      request_future_.wait_for(std::chrono::seconds(10));
  ASSERT_EQ(request_status, std::future_status::ready);
  Request request_store = request_future_.get();
  EXPECT_EQ(request_store.message_id(), response_copy->message_id());
  EXPECT_EQ(request_store.message_response().status().code(), 0);
}

// Tests the functionality of Await() of client when rpc is terminated by the
// server shutdown.
TEST_F(AcsAgentClientReactorTest, TestAwaitOnServerShutdown) {
  // Create a client reactor with a read callback to collect the RpcStatus.
  std::promise<AcsAgentClientReactor::RpcStatus> rpc_status_promise;
  std::future<AcsAgentClientReactor::RpcStatus> rpc_status_future =
      rpc_status_promise.get_future();
  reactor_ = std::make_unique<AcsAgentClientReactor>(
      std::move(stub_),
      [&rpc_status_promise](Response /*response*/,
                            AcsAgentClientReactor::RpcStatus status) {
        rpc_status_promise.set_value(status);
      });
  ASSERT_TRUE(WaitUntil([this]() { return service_.IsReactorCreated(); },
                        absl::Seconds(10)));
  ABSL_VLOG(2) << "Setup done";

  // Start a thread to Await on the client reactor, try to capture the status
  // with the promise.
  std::promise<grpc::Status> status_promise;
  std::future<grpc::Status> status_future = status_promise.get_future();
  std::thread await_thread([this, &status_promise]() {
    status_promise.set_value(reactor_->Await());
  });

  // Shutdown the server.
  std::chrono::system_clock::time_point deadline =
      std::chrono::system_clock::now() + std::chrono::seconds(10);
  server_.Shutdown(deadline);
  server_.Wait();

  // Verify that the status is captured by the promise.
  std::future_status status = status_future.wait_for(std::chrono::seconds(10));
  ASSERT_EQ(status, std::future_status::ready);
  grpc::Status status_value = status_future.get();
  EXPECT_EQ(status_value.error_code(), grpc::StatusCode::UNAVAILABLE);

  AcsAgentClientReactor::RpcStatus rpc_status = rpc_status_future.get();
  EXPECT_EQ(rpc_status, AcsAgentClientReactor::RpcStatus::kRpcClosedByServer);

  await_thread.join();
}

// Tests the functionality of Cancel() of client and Await() would be able to
// capture the right status.
TEST_F(AcsAgentClientReactorTest, TestCancel) {
  // Create a client reactor with a read callback to collect the RpcStatus.
  std::promise<AcsAgentClientReactor::RpcStatus> rpc_status_promise;
  std::future<AcsAgentClientReactor::RpcStatus> rpc_status_future =
      rpc_status_promise.get_future();
  reactor_ = std::make_unique<AcsAgentClientReactor>(
      std::move(stub_),
      [&rpc_status_promise](Response /*response*/,
                            AcsAgentClientReactor::RpcStatus status) {
        rpc_status_promise.set_value(status);
      });
  ASSERT_TRUE(WaitUntil([this]() { return service_.IsReactorCreated(); },
                        absl::Seconds(10)));
  ABSL_LOG(INFO) << "Setup done";

  // Start a thread to Await on the client reactor, try to capture the status
  // with the promise.
  std::promise<grpc::Status> status_promise;
  std::future<grpc::Status> status_future = status_promise.get_future();
  std::thread await_thread([this, &status_promise]() {
    status_promise.set_value(reactor_->Await());
  });

  // Cancel the RPC through the client reactor.
  reactor_->Cancel();
  std::future_status status = status_future.wait_for(std::chrono::seconds(10));
  ASSERT_EQ(status, std::future_status::ready);
  grpc::Status status_value = status_future.get();
  EXPECT_EQ(status_value.error_code(), grpc::StatusCode::CANCELLED);

  AcsAgentClientReactor::RpcStatus rpc_status = rpc_status_future.get();
  EXPECT_EQ(rpc_status, AcsAgentClientReactor::RpcStatus::kRpcClosedByClient);

  await_thread.join();
}

TEST_F(AcsAgentClientReactorTest, TestReactorCanInitializeQuota) {
  // Create a client reactor with a read callback to do nothing.
  reactor_ = std::make_unique<AcsAgentClientReactor>(
      std::move(stub_),
      [](Response /*response*/, AcsAgentClientReactor::RpcStatus /*ok*/) {});
  ASSERT_TRUE(WaitUntil([this]() { return service_.IsReactorCreated(); },
                        absl::Seconds(10)));
  // Add initial metadata to the server after the reactor is created, before the
  // client reactor sends the request.
  service_.AddInitialMetadata("agent-communication-message-rate-limit", "1");
  service_.AddInitialMetadata("agent-communication-bandwidth-limit", "100");
  ABSL_LOG(INFO) << "Setup done";
  // Create a request.
  std::unique_ptr<Request> request = MakeRequestWithBody("body test");
  // Add the request to the reactor.
  ASSERT_TRUE(reactor_->AddRequest(*request));

  ASSERT_TRUE(WaitUntil(
      [this]() {
        return reactor_->GetMessagesPerMinuteQuota().ok() &&
               reactor_->GetBytesPerMinuteQuota().ok();
      },
      absl::Seconds(10)));
  EXPECT_THAT(reactor_->GetMessagesPerMinuteQuota(),
              absl_testing::IsOkAndHolds(1));
  EXPECT_THAT(reactor_->GetBytesPerMinuteQuota(),
              absl_testing::IsOkAndHolds(100));
}

TEST_F(AcsAgentClientReactorTest,
       TestReactorCanInitializeQuotaWithMultipleMetadataWithSameKey) {
  // Create a client reactor with a read callback to do nothing.
  reactor_ = std::make_unique<AcsAgentClientReactor>(
      std::move(stub_),
      [](Response /*response*/, AcsAgentClientReactor::RpcStatus /*ok*/) {});
  ASSERT_TRUE(WaitUntil([this]() { return service_.IsReactorCreated(); },
                        absl::Seconds(10)));
  // Add initial metadata to the server after the reactor is created, before the
  // client reactor sends the request.
  service_.AddInitialMetadata("agent-communication-message-rate-limit", "1");
  service_.AddInitialMetadata("agent-communication-message-rate-limit", "100");
  ABSL_LOG(INFO) << "Setup done";

  // Before the server sends the initial metadata, the client reactor should not
  // be able to get the quota.
  EXPECT_THAT(reactor_->GetMessagesPerMinuteQuota(),
              absl_testing::StatusIs(absl::StatusCode::kFailedPrecondition,
                                     "stream not initialized."));
  EXPECT_THAT(reactor_->GetBytesPerMinuteQuota(),
              absl_testing::StatusIs(absl::StatusCode::kFailedPrecondition,
                                     "stream not initialized."));

  // Create a request.
  std::unique_ptr<Request> request = MakeRequestWithBody("body test");
  // Add the request to the reactor.
  ASSERT_TRUE(reactor_->AddRequest(*request));

  ASSERT_TRUE(
      WaitUntil([this]() { return reactor_->GetMessagesPerMinuteQuota().ok(); },
                absl::Seconds(10)));
  EXPECT_THAT(reactor_->GetMessagesPerMinuteQuota(),
              absl_testing::IsOkAndHolds(AnyOf(1, 100)));
}

TEST_F(AcsAgentClientReactorTest,
       TestReactorCanInitializeQuotaWithInvalidMetadata) {
  // Create a client reactor with a read callback to do nothing.
  reactor_ = std::make_unique<AcsAgentClientReactor>(
      std::move(stub_), [](Response /*response*/,
                           AcsAgentClientReactor::RpcStatus /*status*/) {});
  ASSERT_TRUE(WaitUntil([this]() { return service_.IsReactorCreated(); },
                        absl::Seconds(10)));
  service_.AddInitialMetadata("agent-communication-message-rate-limit", "a");
  service_.AddInitialMetadata("agent-communication-bandwidth-limit", "100");
  ABSL_LOG(INFO) << "Setup done";
  // Create a request.
  std::unique_ptr<Request> request = MakeRequestWithBody("body test");
  // Add the request to the reactor.
  ASSERT_TRUE(reactor_->AddRequest(*request));

  ASSERT_TRUE(WaitUntil(
      [this]() {
        return !reactor_->GetMessagesPerMinuteQuota().ok() &&
               reactor_->GetBytesPerMinuteQuota().ok();
      },
      absl::Seconds(10)));
  EXPECT_THAT(
      reactor_->GetMessagesPerMinuteQuota(),
      absl_testing::StatusIs(
          absl::StatusCode::kNotFound,
          "key: agent-communication-message-rate-limit was found in initial "
          "metadata from server but its value was not a valid integer."));
  EXPECT_THAT(reactor_->GetBytesPerMinuteQuota(),
              absl_testing::IsOkAndHolds(100));
}

TEST_F(AcsAgentClientReactorTest,
       TestReactorCanInitializeQuotaWithEmptyMetadata) {
  // Create a client reactor with a read callback to do nothing.
  reactor_ = std::make_unique<AcsAgentClientReactor>(
      std::move(stub_), [](Response /*response*/,
                           AcsAgentClientReactor::RpcStatus /*status*/) {});
  ASSERT_TRUE(WaitUntil([this]() { return service_.IsReactorCreated(); },
                        absl::Seconds(10)));
  ABSL_LOG(INFO) << "Setup done";
  // Create a request.
  std::unique_ptr<Request> request = MakeRequestWithBody("body test");
  // Add the request to the reactor.
  ASSERT_TRUE(reactor_->AddRequest(*request));

  EXPECT_FALSE(WaitUntil(
      [this]() {
        return reactor_->GetMessagesPerMinuteQuota().ok() ||
               reactor_->GetBytesPerMinuteQuota().ok();
      },
      absl::Seconds(3)));
  EXPECT_THAT(
      reactor_->GetMessagesPerMinuteQuota(),
      absl_testing::StatusIs(absl::StatusCode::kNotFound,
                             "The key: agent-communication-message-rate-limit "
                             "was not found in initial metadata from server."));
  EXPECT_THAT(
      reactor_->GetBytesPerMinuteQuota(),
      absl_testing::StatusIs(absl::StatusCode::kNotFound,
                             "The key: agent-communication-bandwidth-limit was "
                             "not found in initial metadata from server."));
}

}  // namespace
}  // namespace agent_communication
