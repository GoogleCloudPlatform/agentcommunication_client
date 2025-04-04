#ifndef THIRD_PARTY_AGENTCOMMUNICATION_CLIENT_CPP_ACS_AGENT_CLIENT_REACTOR_H_
#define THIRD_PARTY_AGENTCOMMUNICATION_CLIENT_CPP_ACS_AGENT_CLIENT_REACTOR_H_

#include <cstdint>
#include <map>
#include <memory>
#include <queue>
#include <string>

#include "proto/agent_communication.grpc.pb.h"
#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "cpp/acs_agent_helper.h"
#include "grpcpp/client_context.h"
#include "grpcpp/support/client_callback.h"
#include "grpcpp/support/status.h"
#include "grpcpp/support/string_ref.h"

namespace agent_communication {

// gRPC callback-based client reactor for the StreamAgentMessages RPC. This
// class is thread-safe.

// This reactor provides a public API to add requests to the buffer of the
// reactor. Only one request message is buffered at a time to allow the caller
// to have precise control the flow of the requests (QPS and BPS). On Read, this
// reactor will try to add a response to the write queue if the read message has
// a MessageBody, and it will call the read_callback_ to process the response.
// On write, this reactor will prioritize the write of ack over message.
class AcsAgentClientReactor
    : public grpc::ClientBidiReactor<
          google::cloud::agentcommunication::v1::StreamAgentMessagesRequest,
          google::cloud::agentcommunication::v1::StreamAgentMessagesResponse> {
 public:
  // Status of the RPC to be passed to the read_callback_ to let the caller know
  // the status of the RPC.
  enum class RpcStatus {
    kRpcOk,  // Rpc is normal.
    kRpcClosedByClient,  // The RPC is force cancelled by the client.
    kRpcClosedByServer,  // The RPC is being terminated by the server.
  };

  // Test only constructor to create client locally without connecting through a
  // channel to a real ACS server.
  explicit AcsAgentClientReactor(
      std::unique_ptr<
          google::cloud::agentcommunication::v1::AgentCommunication::Stub>
          stub,
      absl::AnyInvocable<void(
          google::cloud::agentcommunication::v1::StreamAgentMessagesResponse,
          RpcStatus)>
          read_callback);

  explicit AcsAgentClientReactor(
      std::unique_ptr<
          google::cloud::agentcommunication::v1::AgentCommunication::Stub>
          stub,
      absl::AnyInvocable<void(
          google::cloud::agentcommunication::v1::StreamAgentMessagesResponse,
          RpcStatus)>
          read_callback,
      const AgentConnectionId& agent_connection_id);

  // Creates a stub to connect to the ACS server.
  static std::unique_ptr<
      google::cloud::agentcommunication::v1::AgentCommunication::Stub>
  CreateStub(const std::string& endpoint);

  // Adds new request to the buffer of reactor.
  // Return: boolean on whether an addition of request is successful. This will
  // be false iff there is a write operation in flight.
  bool AddRequest(
      const google::cloud::agentcommunication::v1::StreamAgentMessagesRequest&
          request) ABSL_LOCKS_EXCLUDED(request_mtx_);

  // Returns the quota of (# of messages per minute & # of bytes per minute) for
  // the agent to send messages to the server. These values are retrieved from
  // server's initial metadata.
  absl::StatusOr<uint64_t> GetMessagesPerMinuteQuota()
      ABSL_LOCKS_EXCLUDED(status_mtx_);
  absl::StatusOr<uint64_t> GetBytesPerMinuteQuota()
      ABSL_LOCKS_EXCLUDED(status_mtx_);

  // Waits for the RPC termination status.
  // This function will listen to the OnDone callback and return the status.
  grpc::Status Await() ABSL_LOCKS_EXCLUDED(status_mtx_);

  // Cancels the RPC if it is not terminated yet.
  bool Cancel() ABSL_LOCKS_EXCLUDED(status_mtx_);

  ~AcsAgentClientReactor();

 private:
  // Override methods of ClientBidiReactor. These methods are called by gRPC
  // framework. OnWriteDone is called when the write operation is done.
  // OnReadDone is called when the read operation is done.
  // OnReadInitialMetadataDone is called when the read initial metadata from
  // server operation is done. OnDone is called when the RPC is terminated.
  void OnWriteDone(bool ok) ABSL_LOCKS_EXCLUDED(request_mtx_) override;
  void OnReadDone(bool ok) ABSL_LOCKS_EXCLUDED(request_mtx_)
      ABSL_LOCKS_EXCLUDED(status_mtx_) override;
  void OnReadInitialMetadataDone(bool ok)
      ABSL_LOCKS_EXCLUDED(status_mtx_) override;
  void OnDone(const grpc::Status& status)
      ABSL_LOCKS_EXCLUDED(status_mtx_) override;

  // Gets the integer value of the key from the initial metadata of server.
  template <typename T>
  absl::StatusOr<T> GetIntValueFromInitialMetadata(
      const std::multimap<grpc::string_ref, grpc::string_ref>& metadata,
      const std::string& key);

  // Adds a response to the queue of ack_buffer_. This function will be called
  // when a message with MessageBody type is received from the server in
  // OnReadDone().
  void Ack(std::string message_id) ABSL_LOCKS_EXCLUDED(request_mtx_);

  // Triggers a StartWrite operation if there is no write operation in flight.
  // This function will be called when a new request is added to the buffer of
  // reactor, a new ack is added to the buffer, or a write operation is
  // completed.
  void NextWrite() ABSL_EXCLUSIVE_LOCKS_REQUIRED(request_mtx_);

  std::unique_ptr<
      google::cloud::agentcommunication::v1::AgentCommunication::Stub>
      stub_;
  grpc::ClientContext context_;

  // Callback invoked in OnReadDone to process the response.
  // Injected during construction to allow the caller of this class to
  // control how to process the messages from the server.
  absl::AnyInvocable<void(
      google::cloud::agentcommunication::v1::StreamAgentMessagesResponse,
      RpcStatus)>
      read_callback_;
  // Buffer to store the response in StartRead.
  google::cloud::agentcommunication::v1::StreamAgentMessagesResponse response_;

  // Mutex to protect variables related to the termination of the RPC.
  absl::Mutex status_mtx_;
  // The final status of the RPC. To be returned by Await() for consumption of
  // the client.
  grpc::Status rpc_final_status_ ABSL_GUARDED_BY(status_mtx_);
  // Whether the RPC has been terminated. This is to indicate to the Await()
  // function that the RPC has been terminated and it can return the final
  // status.
  bool rpc_done_ ABSL_GUARDED_BY(status_mtx_) = false;

  // Whether the RPC has been cancelled forcefully by Cancel().
  // This is to indicate to the read_callback_ that the RPC has been
  // cancelled by the user/client.
  bool rpc_cancelled_by_client_ ABSL_GUARDED_BY(status_mtx_) = false;

  // Quota of (# of messages per minute & # of bytes per minute) for the agent
  // to send messages to the server. This value is retrieved from server's
  // initial metadata.
  absl::StatusOr<uint64_t> messages_per_minute_quota_ ABSL_GUARDED_BY(
      status_mtx_) = absl::FailedPreconditionError("stream not initialized.");
  absl::StatusOr<uint64_t> bytes_per_minute_quota_ ABSL_GUARDED_BY(
      status_mtx_) = absl::FailedPreconditionError("stream not initialized.");

  // Keys for the quota in the initial metadata of server.
  inline static constexpr char kMessagesPerMinuteQuotaKey[] =
      "agent-communication-message-rate-limit";
  inline static constexpr char kBytesPerMinuteQuotaKey[] =
      "agent-communication-bandwidth-limit";

  // Mutex to protect all variables related to write operations.
  absl::Mutex request_mtx_;
  // Whether there is a write operation in flight.
  bool writing_ ABSL_GUARDED_BY(request_mtx_) = false;
  // Raw pointer to the message request to be written to the stream. It can
  // point to either a RegisterConnection, a MessageResponse, or a MessageBody
  // request. This is to be passed into StartWrite() for writing to the stream.
  google::cloud::agentcommunication::v1::StreamAgentMessagesRequest* request_
      ABSL_GUARDED_BY(request_mtx_) = nullptr;
  // Buffer to store the RegisterConnection or MessageBody request added by the
  // client.
  std::unique_ptr<
      google::cloud::agentcommunication::v1::StreamAgentMessagesRequest>
      msg_request_ ABSL_GUARDED_BY(request_mtx_) = nullptr;
  // Buffer to store the MessageResponse that is inserted during OnReadDone for
  // each MessageBody received from the server.
  std::queue<std::unique_ptr<
      google::cloud::agentcommunication::v1::StreamAgentMessagesRequest>>
      ack_buffer_ ABSL_GUARDED_BY(request_mtx_);
};

}  // namespace agent_communication

#endif  // THIRD_PARTY_AGENTCOMMUNICATION_CLIENT_CPP_ACS_AGENT_CLIENT_REACTOR_H_
