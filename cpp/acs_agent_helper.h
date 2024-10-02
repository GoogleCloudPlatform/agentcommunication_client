#ifndef THIRD_PARTY_AGENTCOMMUNICATION_CLIENT_CPP_ACS_AGENT_HELPER_H_
#define THIRD_PARTY_AGENTCOMMUNICATION_CLIENT_CPP_ACS_AGENT_HELPER_H_

#include <memory>
#include <string>
#include <utility>

#include "google/cloud/agentcommunication/v1/agent_communication.pb.h"

namespace agent_communication {

// Struct to hold the connection id of the agent. The fields are used to
// construct the metadata of the gRPC context.
struct AgentConnectionId {
  std::string token;
  std::string resource_id;
  std::string channel_id;
  std::string location;
};
// Creates an ack request for the message with the given message id.
std::unique_ptr<
    google::cloud::agentcommunication::v1::StreamAgentMessagesRequest>
MakeAck(std::string message_id);

// Creates a request with the given message id and response status.
std::unique_ptr<
    google::cloud::agentcommunication::v1::StreamAgentMessagesRequest>
MakeRequestWithResponse(std::string message_id, google::rpc::Status status);
}  // namespace agent_communication

#endif  // THIRD_PARTY_AGENTCOMMUNICATION_CLIENT_CPP_ACS_AGENT_HELPER_H_