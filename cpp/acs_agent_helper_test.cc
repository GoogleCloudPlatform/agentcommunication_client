#include "cpp/acs_agent_helper.h"

#include <memory>
#include <string>

#include "gtest/gtest.h"

namespace agent_communication {

// Alias of the protobuf message types used in the ACS Agent Communication
// service in a .cc file.
using Request =
    ::google::cloud::agentcommunication::v1::StreamAgentMessagesRequest;
using Response =
    ::google::cloud::agentcommunication::v1::StreamAgentMessagesResponse;

TEST(AcsAgentHelperTest, MakeAck) {
  std::string message_id = "message_id";
  std::unique_ptr<Request> request = MakeAck(message_id);
  EXPECT_EQ(request->message_id(), message_id);
  EXPECT_EQ(request->message_response().status().code(), 0);
}

TEST(AcsAgentHelperTest, MakeRequestWithResponse) {
  std::string message_id = "message_id";
  google::rpc::Status status;
  status.set_code(1);
  status.set_message("message");
  status.add_details()->set_value("details1");
  status.add_details()->set_value("details2");
  std::unique_ptr<Request> request =
      MakeRequestWithResponse(message_id, status);
  EXPECT_EQ(request->message_id(), message_id);
  EXPECT_EQ(request->message_response().status().code(), status.code());
  EXPECT_EQ(request->message_response().status().message(), status.message());
  EXPECT_EQ(request->message_response().status().details(0).value(),
            status.details(0).value());
  EXPECT_EQ(request->message_response().status().details(1).value(),
            status.details(1).value());
}

TEST(AcsAgentHelperTest, MakeAckResponse) {
  std::string message_id = "message_id";
  std::unique_ptr<Response> response = MakeAckResponse(message_id);
  EXPECT_EQ(response->message_id(), message_id);
  EXPECT_EQ(response->message_response().status().code(), 0);
}

TEST(AcsAgentHelperTest, MakeResponseWithResponse) {
  std::string message_id = "message_id";
  google::rpc::Status status;
  status.set_code(1);
  status.set_message("message");
  status.add_details()->set_value("details1");
  status.add_details()->set_value("details2");
  std::unique_ptr<Response> response =
      MakeResponseWithResponse(message_id, status);
  EXPECT_EQ(response->message_id(), message_id);
  EXPECT_EQ(response->message_response().status().details(0).value(),
            status.details(0).value());
  EXPECT_EQ(response->message_response().status().details(1).value(),
            status.details(1).value());
}

TEST(AcsAgentHelperTest, MakeRequestWithRegistration) {
  std::string message_id = "message_id";
  std::string channel_id = "channel_id";
  std::string resource_id = "resource_id";
  std::unique_ptr<Request> request =
      MakeRequestWithRegistration(message_id, channel_id, resource_id);
  EXPECT_EQ(request->message_id(), message_id);
  EXPECT_EQ(request->register_connection().channel_id(), channel_id);
  EXPECT_EQ(request->register_connection().resource_id(), resource_id);
}

}  // namespace agent_communication
