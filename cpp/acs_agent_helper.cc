#include "third_party/agentcommunication_client/cpp/acs_agent_helper.h"

#include <memory>
#include <string>
#include <utility>

#include "google/rpc/status.proto.h"

namespace agent_communication {

// Alias of the protobuf message types used in the ACS Agent Communication
// service in a .cc file.
using Request =
    ::google::cloud::agentcommunication::v1::StreamAgentMessagesRequest;
using Response =
    ::google::cloud::agentcommunication::v1::StreamAgentMessagesResponse;

std::unique_ptr<Request> MakeAck(std::string message_id) {
  google::rpc::Status status;
  status.set_code(0);
  return MakeRequestWithResponse(std::move(message_id), std::move(status));
}

std::unique_ptr<Request> MakeRequestWithResponse(std::string message_id,
                                                 google::rpc::Status status) {
  std::unique_ptr<Request> request = std::make_unique<Request>();
  request->set_message_id(std::move(message_id));
  request->mutable_message_response()->mutable_status()->CopyFrom(status);
  return request;
}

}  // namespace agent_communication
