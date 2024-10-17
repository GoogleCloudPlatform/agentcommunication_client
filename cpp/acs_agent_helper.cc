#include "third_party/agentcommunication_client/cpp/acs_agent_helper.h"

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "google/rpc/status.proto.h"
#include "third_party/absl/log/absl_log.h"
#include "third_party/absl/status/status.h"
#include "third_party/absl/status/statusor.h"
#include "third_party/absl/strings/str_cat.h"
#include "third_party/absl/strings/str_split.h"
#include "third_party/absl/strings/string_view.h"
#include "third_party/curl/curl.h"
#include "third_party/curl/easy.h"

namespace agent_communication {

namespace {
// Internal helper functions.

// Callback function for curl to write the response to the output string.
// Input: contents: the response data, size: the size of each element, nmemb:
// the number of elements, output: the output string.
// Returns: the number of bytes processed.
static size_t WriteCallback(void* contents, size_t size, size_t nmemb,
                            std::string* output) {
  size_t total_size = size * nmemb;
  output->append((char*)contents, total_size);
  return total_size;
}

}  // namespace

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
  auto request = std::make_unique<Request>();
  request->set_message_id(std::move(message_id));
  request->mutable_message_response()->mutable_status()->CopyFrom(status);
  return request;
}

std::unique_ptr<Request> MakeRequestWithRegistration(std::string message_id,
                                                     std::string channel_id,
                                                     std::string resource_id) {
  auto request = std::make_unique<Request>();
  request->set_message_id(std::move(message_id));
  google::cloud::agentcommunication::v1::RegisterConnection
      registration_connection;
  registration_connection.set_channel_id(std::move(channel_id));
  registration_connection.set_resource_id(std::move(resource_id));
  *request->mutable_register_connection() = std::move(registration_connection);
  return request;
}

std::unique_ptr<Response> MakeAckResponse(std::string message_id) {
  google::rpc::Status status;
  status.set_code(0);
  return MakeResponseWithResponse(std::move(message_id), std::move(status));
}

std::unique_ptr<Response> MakeResponseWithResponse(std::string message_id,
                                                   google::rpc::Status status) {
  auto response = std::make_unique<Response>();
  response->set_message_id(std::move(message_id));
  response->mutable_message_response()->mutable_status()->CopyFrom(status);
  return response;
}

absl::StatusOr<std::string> CurlHttpGet(const std::string& url,
                                        const std::string& header) {
  CURL* curl;
  CURLcode res;
  std::string read_buffer;
  curl = curl_easy_init();
  if (curl == nullptr) {
    ABSL_LOG(ERROR) << "Failed to initialize curl.";
    return absl::InternalError("Failed to initialize curl.");
  }

  // Set URL.
  res = curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  if (res != CURLE_OK) {
    ABSL_LOG(ERROR) << "Failed to set URL: " << url;
    curl_easy_cleanup(curl);
    return absl::InternalError(
        absl::StrCat("Failed to set URL: ", url, " with error: ",
                     curl_easy_strerror(res)));
  }

  // Set header.
  struct curl_slist* headers = nullptr;
  headers = curl_slist_append(headers, header.c_str());
  res = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  if (res != CURLE_OK || headers == nullptr) {
    ABSL_LOG(ERROR) << "Failed to set header: " << header;
    curl_easy_cleanup(curl);
    if (headers != nullptr) {
      curl_slist_free_all(headers);
    }
    return absl::InternalError(
        absl::StrCat("Failed to set header: ", header, " with error: ",
                     curl_easy_strerror(res)));
  }

  // Set the write callback function and its data.
  // No need to check the return value as they will both return CURLE_OK.
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &read_buffer);

  res = curl_easy_perform(curl);
  curl_easy_cleanup(curl);
  curl_slist_free_all(headers);
  if (res != CURLE_OK) {
    ABSL_LOG(ERROR) << "curl_easy_perform() failed: "
                    << curl_easy_strerror(res);
    return absl::InternalError(absl::StrCat(
        "curl_easy_perform() failed with error: ", curl_easy_strerror(res)));
  }
  ABSL_VLOG(1) << "Got metadata for key: " << url
               << " and its value is: " << read_buffer;
  return read_buffer;
}

absl::StatusOr<std::string> GetMetadata(absl::string_view key) {
  return CurlHttpGet(
      absl::StrCat("http://metadata.google.internal/computeMetadata/v1/", key),
      "Metadata-Flavor: Google");
}

absl::StatusOr<AgentConnectionId> GenerateAgentConnectionId(
    std::string channel_id, bool regional) {
  absl::StatusOr<std::string> token = GetMetadata(
      "instance/service-accounts/default/"
      "identity?audience=agentcommunication.googleapis.com&format=full");
  if (!token.ok()) {
    return token.status();
  }
  ABSL_LOG(INFO) << "Successfully got token from metadata service: " << *token;

  absl::StatusOr<std::string> numeric_project_id_zone =
      GetMetadata("instance/zone");
  if (!numeric_project_id_zone.ok()) {
    return numeric_project_id_zone.status();
  }
  std::vector<std::string> numeric_project_id_zone_vector =
      absl::StrSplit(*numeric_project_id_zone, '/');
  if (numeric_project_id_zone_vector.size() != 4) {
    ABSL_LOG(ERROR)
        << "Wrong format of numeric_project_id_zone from metadata service: "
        << numeric_project_id_zone;
    return absl::InternalError(absl::StrCat(
        "Wrong format of numeric_project_id_zone from metadata service: ",
        *numeric_project_id_zone));
  }
  ABSL_LOG(INFO)
      << "Successfully got numeric_project_id_zone from metadata service: "
      << *numeric_project_id_zone;
  const std::string& zone = numeric_project_id_zone_vector[3];

  // Deduce the location from the zone.
  // If regional is true, the location is the zone without the last two
  // characters. Otherwise, the location is the zone itself.
  // Example: zone: us-central1-a -> region: us-central1
  size_t last_hyphen_index = zone.find_last_of('-');
  if (last_hyphen_index == std::string::npos) {
    return absl::InternalError(
        absl::StrCat("Wrong format of zone from metadata service: ", zone));
  }
  std::string location = regional ? zone.substr(0, last_hyphen_index) : zone;

  absl::StatusOr<std::string> instance_id = GetMetadata("instance/id");
  if (!instance_id.ok()) {
    return instance_id.status();
  }
  ABSL_LOG(INFO) << "Successfully got instance_id from metadata service: "
                 << *instance_id;

  std::string resource_id =
      absl::StrCat(*numeric_project_id_zone, "/instances/", *instance_id);
  return AgentConnectionId{.token = std::move(*token),
                           .resource_id = std::move(resource_id),
                           .channel_id = std::move(channel_id),
                           .location = std::move(location)};
}

}  // namespace agent_communication
