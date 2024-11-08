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
#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "cpp/acs_agent_client_reactor.h"
#include "cpp/acs_agent_helper.h"
#include "grpc/include/grpcpp/support/status.h"

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
          read_callback,
      absl::AnyInvocable<std::unique_ptr<
          google::cloud::agentcommunication::v1::AgentCommunication::Stub>()>
          stub_generator);

  // Sends a StreamAgentMessagesRequest to the server.
  // It will automatically retry if the request was not acknowledged by the
  // server or the server returns a ResourceExhausted error.
  // We pass by non-const reference to allow the update of the message_id on
  // every retry, without the need to create a new StreamAgentMessagesRequest
  // object. Returns status of the send request.
  absl::Status AddRequest(
      google::cloud::agentcommunication::v1::StreamAgentMessagesRequest&
          request) ABSL_LOCKS_EXCLUDED(request_delivery_status_mtx_)
      ABSL_LOCKS_EXCLUDED(reactor_mtx_);

  // Sends a MessageBody to the server. The input parameter
  // message_body will be moved to create a StreamAgentMessagesRequest and then
  // call AddRequest().
  // Returns status of AddRequest().
  absl::Status SendMessage(
      google::cloud::agentcommunication::v1::MessageBody message_body)
      ABSL_LOCKS_EXCLUDED(request_delivery_status_mtx_)
          ABSL_LOCKS_EXCLUDED(reactor_mtx_);

  // Checks if the client is dead. If the caller of this class has failure of
  // sending or receiving messages, it can call this function to check if the
  // client is dead and needs a restart. If the client is dead, the caller can
  // call Shutdown() or directly invoke destructor to clean up the client. If
  // the client is not dead, the caller can retry sending messages later with a
  // self-determined backoff mechanism.
  bool IsDead() ABSL_LOCKS_EXCLUDED(reactor_mtx_);

  // Shuts down the client by joining the restart client thread and the
  // read_response_thread_, and then cancel the RPC.
  void Shutdown() ABSL_LOCKS_EXCLUDED(reactor_mtx_)
      ABSL_LOCKS_EXCLUDED(request_delivery_status_mtx_);

  ~AcsAgentClient() { Shutdown(); }

 private:
  AcsAgentClient(
      AgentConnectionId connection_id,
      absl::AnyInvocable<void(
          google::cloud::agentcommunication::v1::StreamAgentMessagesResponse)>
          read_callback,
      absl::AnyInvocable<std::unique_ptr<
          google::cloud::agentcommunication::v1::AgentCommunication::Stub>()>
          stub_generator)
      : connection_id_(std::move(connection_id)),
        stub_generator_(std::move(stub_generator)),
        read_callback_(std::move(read_callback)) {}

  // Initializes the client by registering the connection.
  absl::Status Init() ABSL_EXCLUSIVE_LOCKS_REQUIRED(reactor_mtx_)
      ABSL_LOCKS_EXCLUDED(request_delivery_status_mtx_);

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
          request) ABSL_LOCKS_EXCLUDED(request_delivery_status_mtx_)
      ABSL_LOCKS_EXCLUDED(reactor_mtx_);

  // Registers the connection with the server. Note this function holds the
  // reactor_mtx_ lock entire time intentionally to keep the client from
  // sending any other requests.
  absl::Status RegisterConnection(
      const google::cloud::agentcommunication::v1::StreamAgentMessagesRequest&
          request) ABSL_LOCKS_EXCLUDED(request_delivery_status_mtx_)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(reactor_mtx_);

  // Processes Responses from the server. Acknowledges the response from server
  // and then calls the read_callback_ to process the message. This function
  // will be executed in the read_response_thread_.
  void ClientReadMessage() ABSL_LOCKS_EXCLUDED(response_read_mtx_)
      ABSL_LOCKS_EXCLUDED(request_delivery_status_mtx_);

  // Checks if the ClientReadMessage() should be woken up.
  bool ShouldWakeUpClientReadMessage()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(response_read_mtx_);

  // Callback invoked in OnReadDone of reactor to process the Response.
  // If OnReadDone(true), wakes up the read_response_thread_ and passes the
  // received Response to be handled by the ClientReadMessage(). In this way,
  // the reactor's OnReadDone call will return fast, and the actual processing
  // of the Response is delegated to the read_response_thread_.
  // If OnReadDone(false), the RPC is terminated, wakes up the
  // restart_client_thread_ to re-initialize the client.
  void ReactorReadCallback(
      google::cloud::agentcommunication::v1::StreamAgentMessagesResponse
          response,
      bool ok) ABSL_LOCKS_EXCLUDED(response_read_mtx_)
      ABSL_LOCKS_EXCLUDED(reactor_mtx_);

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

  // Restarts the client when the RPC is terminated. This function will be
  // executed in the restart_client_thread_.
  // Listens to the ReactorReadCallback(). When OnReadDone(false), this function
  // will be waken up to restart the client, i.e., create a new stub and
  // connection id, re-create the stream, and register the connection.
  void RestartClient() ABSL_LOCKS_EXCLUDED(reactor_mtx_)
      ABSL_LOCKS_EXCLUDED(request_delivery_status_mtx_);

  // Generates a new stub and connection id and returns the stub, called within
  // the restart_client_thread_.
  std::unique_ptr<
      google::cloud::agentcommunication::v1::AgentCommunication::Stub>
  GenerateConnectionIdAndStub() ABSL_EXCLUSIVE_LOCKS_REQUIRED(reactor_mtx_);

  // Creates a unique message id. Currently, it is "{random
  // number}-{current_timestamp}". The requirement of the uniqueness from ACS
  // server is not very strict: We just need to make sure the message id is
  // unique within a couple of seconds for each agent.
  std::string CreateMessageUuid() ABSL_EXCLUSIVE_LOCKS_REQUIRED(reactor_mtx_);

  // Set the value of the promise and remove the promise from the map. This
  // function is used to ensure:
  // 1. If the promise is not set due to any reason but we want to remove it, we
  // will set the value and then safely destroy the promise.
  // 2. Whenever we set the value of the promise, we safely destroy the promise
  // so that it never gets set the value again.
  void SetValueAndRemovePromise(const std::string& message_id,
                                absl::Status status)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(request_delivery_status_mtx_);

  absl::BitGen gen_ ABSL_GUARDED_BY(reactor_mtx_);

  // Dedicated to reading the Response from the server.
  std::thread read_response_thread_;

  AgentConnectionId connection_id_ ABSL_GUARDED_BY(reactor_mtx_);

  // Mutex to protect the reactor_ and other variables that are only accessed
  // by the reactor_ for write operations.
  absl::Mutex reactor_mtx_ ABSL_ACQUIRED_BEFORE(request_delivery_status_mtx_);

  // Generates a new stub when the client needs to restart the client. We have a
  // default implementation, this function pointer allows us to pass in a custom
  // implementation for testing purposes and other use cases.
  absl::AnyInvocable<std::unique_ptr<
      google::cloud::agentcommunication::v1::AgentCommunication::Stub>()>
      stub_generator_ = nullptr;

  // Reactor to handle the gRPC communication with the server.
  std::unique_ptr<AcsAgentClientReactor> reactor_ ABSL_GUARDED_BY(reactor_mtx_);

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
    // Stream not initialized, waiting for the first registration request.
    kStreamNotInitialized,
    // The RPC is temporarily down, waiting for a restart.
    kStreamTemporarilyDown,
    // The RPC failed to be initialized.
    kStreamFailedToInitialize,
    // The client is being shutdown.
    kShutdown,
  };

  // State of the client read thread.
  ClientState client_read_state_ ABSL_GUARDED_BY(response_read_mtx_) =
      ClientState::kReady;

  // State of the stream/RPC.
  ClientState stream_state_ ABSL_GUARDED_BY(reactor_mtx_) =
      ClientState::kStreamNotInitialized;

  std::thread restart_client_thread_;

  // Buffer to store the Response read from OnReadDone of reactor.
  std::queue<google::cloud::agentcommunication::v1::StreamAgentMessagesResponse>
      msg_responses_ ABSL_GUARDED_BY(response_read_mtx_);
};

}  // namespace agent_communication

#endif  // THIRD_PARTY_AGENTCOMMUNICATION_CLIENT_CPP_ACS_AGENT_CLIENT_H_
