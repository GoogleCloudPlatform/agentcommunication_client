#ifndef THIRD_PARTY_AGENTCOMMUNICATION_CLIENT_CPP_ACS_AGENT_CLIENT_H_
#define THIRD_PARTY_AGENTCOMMUNICATION_CLIENT_CPP_ACS_AGENT_CLIENT_H_

#include <future>
#include <memory>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include "google/cloud/agentcommunication/v1/agent_communication.grpc.pb.h"
#include "google/cloud/agentcommunication/v1/agent_communication.proto.h"
#include "third_party/absl/base/thread_annotations.h"
#include "third_party/absl/functional/any_invocable.h"
#include "third_party/absl/random/random.h"
#include "third_party/absl/status/status.h"
#include "third_party/absl/status/statusor.h"
#include "third_party/absl/synchronization/mutex.h"
#include "third_party/agentcommunication_client/cpp/acs_agent_client_reactor.h"
#include "third_party/agentcommunication_client/cpp/acs_agent_helper.h"
#include "third_party/grpc/include/grpcpp/support/status.h"

namespace agent_communication {

// AcsAgentClient is a client library for Agent Communication Service (ACS).
// This class is thread-safe.

// This class provides a public API to send and receive messages from the ACS
// server. It uses a gRPC client reactor to handle the communication with the
// server. After the creation and initialization of this class, to send a
// message to the server, the caller can send messages to the server by calling
// SendMessage() or AddRequest(). On read, the caller can register a callback to
// process the response from the server during the creation of this class.
class AcsAgentClient {
 public:
  // Factory method to create a client.
  static absl::StatusOr<std::unique_ptr<AcsAgentClient>> Create(
      std::unique_ptr<
          google::cloud::agentcommunication::v1::AgentCommunication::Stub>
          stub,
      AgentConnectionId agent_connection_id,
      absl::AnyInvocable<void(
          google::cloud::agentcommunication::v1::StreamAgentMessagesResponse)>
          read_callback);

  // Sends a StreamAgentMessagesRequest to the server.
  // It will automatically retry if the request was not acknowledged by the
  // server or the server returns a ResourceExhausted error.
  // We pass by non-const reference to allow the update of the message_id on
  // every retry, without the need to create a new StreamAgentMessagesRequest
  // object. Returns status of the send request.
  absl::Status AddRequest(
      google::cloud::agentcommunication::v1::StreamAgentMessagesRequest&
          request) ABSL_LOCKS_EXCLUDED(request_delivery_status_mtx_);

  // Sends a MessageBody to the server. The input parameter
  // message_body will be moved to create a StreamAgentMessagesRequest and then
  // call AddRequest().
  // Returns status of AddRequest().
  absl::Status SendMessage(
      google::cloud::agentcommunication::v1::MessageBody message_body)
      ABSL_LOCKS_EXCLUDED(request_delivery_status_mtx_);

  // Cancels the RPC.
  void CancelReactor() { reactor_->Cancel(); }

  // Awaits the termination of the RPC. Returns the final status of the RPC.
  grpc::Status AwaitReactor() { return reactor_->Await(); }

  ~AcsAgentClient() { Shutdown(); }

 private:
  AcsAgentClient(AgentConnectionId connection_id)
      : connection_id_(std::move(connection_id)) {}

  // Initializes the client. Spins up read_response_thread_ to process the
  // responses from the server and sends the registration request.
  absl::Status Init();

  // Adds a request to the buffer of the reactor and waits for the response from
  // the server.
  // After the message has been queued in the reactor, it will wait for the
  // response from the server by creating a promise and a future associated with
  // the message id. The promise will be set by the AckOnSuccessfulDelivery()
  // function when the response from the server is received.
  // This function will retry if the reactor's existing buffer is full.
  // Returns: status of the send request.
  absl::Status AddRequestAndWaitForResponse(
      const google::cloud::agentcommunication::v1::StreamAgentMessagesRequest&
          request) ABSL_LOCKS_EXCLUDED(request_delivery_status_mtx_);

  // Processes Responses from the server. Acknowledges the response from server
  // and then calls the read_callback_ to process the message. This function
  // will be executed in the read_response_thread_.
  void ClientReadMessage() ABSL_LOCKS_EXCLUDED(response_read_mtx_);

  // Checks if the ClientReadMessage() should be woken up.
  bool ShouldWakeUpClientReadMessage()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(response_read_mtx_);

  // Callback invoked in OnReadDone of reactor to process the Response.
  // Wakes up the read_response_thread_ and passes the received Response to be
  // handled by the ClientReadMessage(). In this way, the reactor's OnReadDone
  // call will return fast, and the actual processing of the Response is
  // delegated to the read_response_thread_.
  void ReactorReadCallback(
      google::cloud::agentcommunication::v1::StreamAgentMessagesResponse
          response) ABSL_LOCKS_EXCLUDED(response_read_mtx_);

  // Acknowledges the Response from the server for a successful delivery of a
  // Request. ACS server normally sends a MessageResponse as an acknowledgement
  // for each received Request that has a MessageBody or RegisterConnection.
  // Within AddRequestAndWaitForResponse(), the client creates a promise and
  // a future associated with the message id. The promise will be set by this
  // AckOnSuccessfulDelivery() function when the MessageResponse from the server
  // associated with the message id is received.
  void AckOnSuccessfulDelivery(
      const google::cloud::agentcommunication::v1::StreamAgentMessagesResponse&
          response) ABSL_LOCKS_EXCLUDED(request_delivery_status_mtx_);

  // Shuts down the client by cancelling the RPC and joining the
  // read_response_thread_.
  void Shutdown();

  // Creates a unique message id. Currently, it is "{random
  // number}-{current_timestamp}". The requirement of the uniqueness from ACS
  // server is not very strict: We just need to make sure the message id is
  // unique within a couple of seconds for each agent.
  std::string CreateMessageUuid();

  // Set the value of the promise and remove the promise from the map. This
  // function is used to ensure:
  // 1. If the promise is not set due to any reason but we want to remove it, we
  // will set the value and then safely destroy the promise.
  // 2. Whenever we set the value of the promise, we safely destroy the promise
  // so that it never gets set the value again.
  void SetValueAndRemovePromise(const std::string& message_id,
                                absl::Status status)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(request_delivery_status_mtx_);

  // Random number generator for creating message id.
  absl::BitGen gen_;

  // Dedicated to reading the Response from the server.
  std::thread read_response_thread_;

  // Connection id of the agent.
  AgentConnectionId connection_id_;

  // Reactor to handle the gRPC communication with the server.
  std::unique_ptr<AcsAgentClientReactor> reactor_;

  // Callback injected by the client to process the messages from the server.
  absl::AnyInvocable<void(
      google::cloud::agentcommunication::v1::StreamAgentMessagesResponse)>
      read_callback_;

  // Mutex to protect the attempted_requests_responses_sub_.
  absl::Mutex request_delivery_status_mtx_;

  // Key: message id of each attempted request. Value: promise to be set when
  // the response from the server is received in AckOnSuccessfulDelivery().
  std::unordered_map<std::string, std::promise<absl::Status>> ABSL_GUARDED_BY(
      request_delivery_status_mtx_) attempted_requests_responses_sub_;

  // Mutex that serves as a channel to pass the message from OnReadDone of
  // reactor to the ClientReadMessage().
  absl::Mutex response_read_mtx_;

  // State of the client.
  enum class ClientState {
    // The client is ready to read any Response from the server.
    kReady,
    // The client is being shutdown.
    kShutdown,
  };
  ClientState client_state_ ABSL_GUARDED_BY(response_read_mtx_) =
      ClientState::kReady;

  // Buffer to store the Response read from OnReadDone of reactor.
  std::queue<google::cloud::agentcommunication::v1::StreamAgentMessagesResponse>
      msg_responses_ ABSL_GUARDED_BY(response_read_mtx_);
};

}  // namespace agent_communication

#endif  // THIRD_PARTY_AGENTCOMMUNICATION_CLIENT_CPP_ACS_AGENT_CLIENT_H_
