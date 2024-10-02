#include "third_party/agentcommunication_client/cpp/acs_agent_helper.h"

#include <memory>
#include <random>
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

// Returns a random number between 1 and 1000.
// TODO: use a more robust UUID generator.
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

std::unique_ptr<Request> MakeRequestWithBody(std::string message) {
  std::unique_ptr<Request> request = std::make_unique<Request>();
  request->set_message_id(randomNumber());
  request->mutable_message_body()->mutable_body()->set_value(
      std::move(message));
  return request;
}

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

std::unique_ptr<Response> MakeAckResponse(std::string message_id) {
  google::rpc::Status status;
  status.set_code(0);
  return MakeResponseWithResponse(std::move(message_id), std::move(status));
}

std::unique_ptr<Response> MakeResponseWithResponse(std::string message_id,
                                                   google::rpc::Status status) {
  std::unique_ptr<Response> response = std::make_unique<Response>();
  response->set_message_id(std::move(message_id));
  response->mutable_message_response()->mutable_status()->CopyFrom(status);
  return response;
}

std::unique_ptr<Response> MakeResponseWithBody(std::string message) {
  std::unique_ptr<Response> response = std::make_unique<Response>();
  response->set_message_id(randomNumber());
  response->mutable_message_body()->mutable_body()->set_value(
      std::move(message));
  return response;
}

}  // namespace agent_communication
