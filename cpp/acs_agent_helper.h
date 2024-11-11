#ifndef THIRD_PARTY_AGENTCOMMUNICATION_CLIENT_CPP_ACS_AGENT_HELPER_H_
#define THIRD_PARTY_AGENTCOMMUNICATION_CLIENT_CPP_ACS_AGENT_HELPER_H_

#include <memory>
#include <string>

#include "agent_communication.pb.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

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

// Queries the response of a http get request through libcurl.
absl::StatusOr<std::string> CurlHttpGet(const std::string& url,
                                        const std::string& header);

// Calls CurlHttpGet to query the metadata service.
// Input: key: the key of the metadata to query.
// Returns: the value for the given key from the metadata service.
absl::StatusOr<std::string> GetMetadata(absl::string_view key);

// Generates the agent connection id for the given channel id.
// This function queries the metadata service to get the ACS token for a
// specific VM and instance UUID (project/zone/instance number) from the
// metadata service, and then construct the AgentConnectionId.
// Input: channel_id: the channel id of the agent. regional: whether to use
// regional ACS endpoint.
absl::StatusOr<AgentConnectionId> GenerateAgentConnectionId(
    std::string channel_id, bool regional);

// Creates a request with the given message id and response status.
std::unique_ptr<
    google::cloud::agentcommunication::v1::StreamAgentMessagesRequest>
MakeRequestWithResponse(std::string message_id, google::rpc::Status status);

// Creates a Registration request with the given message id, channel id and
// resource id.
std::unique_ptr<
    google::cloud::agentcommunication::v1::StreamAgentMessagesRequest>
MakeRequestWithRegistration(std::string message_id, std::string channel_id,
                            std::string resource_id);

// Creates an ack response for the message with the given message id.
std::unique_ptr<
    google::cloud::agentcommunication::v1::StreamAgentMessagesResponse>
MakeAckResponse(std::string message_id);

// Creates a response with the given message id and response status.
std::unique_ptr<
    google::cloud::agentcommunication::v1::StreamAgentMessagesResponse>
MakeResponseWithResponse(std::string message_id, google::rpc::Status status);

}  // namespace agent_communication

#endif  // THIRD_PARTY_AGENTCOMMUNICATION_CLIENT_CPP_ACS_AGENT_HELPER_H_
