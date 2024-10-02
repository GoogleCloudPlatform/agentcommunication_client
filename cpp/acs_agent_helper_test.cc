#include "third_party/agentcommunication_client/cpp/acs_agent_helper.h"

#include <memory>
#include <string>

#include "testing/base/public/gunit.h"

namespace agent_communication {

// Alias of the protobuf message types used in the ACS Agent Communication
// service in a .cc file.
using Request =
    ::google::cloud::agentcommunication::v1::StreamAgentMessagesRequest;

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
  EXPECT_EQ(request->message_response().status().code(), 1);
  EXPECT_EQ(request->message_response().status().message(), "message");
  EXPECT_EQ(request->message_response().status().details(0).value(),
            "details1");
  EXPECT_EQ(request->message_response().status().details(1).value(),
            "details2");
}

}  // namespace agent_communication
