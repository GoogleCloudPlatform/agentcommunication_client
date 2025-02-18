#include "cpp/jwt.h"

#include <exception>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/types/span.h"
#include "jwt-cpp/jwt.h"
#include "jwt-cpp/traits/nlohmann-json/defaults.h"
#include "jwt-cpp/traits/nlohmann-json/traits.h"

namespace agent_communication {
using decoded_jwt_t = ::jwt::decoded_jwt<jwt::traits::nlohmann_json>;

namespace {
// Anonymous helper function to decode a JWT token.
static absl::StatusOr<decoded_jwt_t> DecodeJwt(const std::string& token) {
  try {
    decoded_jwt_t decoded = jwt::decode(token);
    return decoded;
  } catch (const std::exception& e) {
    return absl::InternalError(
        absl::StrCat("Failed to decode token: ", e.what()));
  }
}
}  // namespace

absl::StatusOr<std::string> GetValueFromTokenPayloadWithKeys(
    const std::string& token, absl::Span<const std::string> keys) {
  try {
    if (keys.empty() || token.empty()) {
      return absl::InvalidArgumentError("Keys/token cannot be empty.");
    }
    absl::StatusOr<decoded_jwt_t> decoded = DecodeJwt(token);
    if (!decoded.ok()) {
      return decoded.status();
    }
    jwt::traits::nlohmann_json::json json_obj = decoded->get_payload_json();
    for (const std::string& key : keys) {
      if (!json_obj.is_object()) {
        return absl::InternalError(
            absl::StrCat("Token has an invalid format: ", json_obj.dump()));
      }
      if (!json_obj.contains(key)) {
        return absl::InternalError(absl::StrFormat(
            "Token: %s does not contain key: %s.", json_obj.dump(), key));
      }
      json_obj = json_obj[key];
    }
    if (json_obj.is_number()) {
      return json_obj.dump();
    }
    if (json_obj.is_string()) {
      return json_obj.get<std::string>();
    }
    return absl::InternalError(absl::StrFormat(
        "Token %s does not have a string/number value for key (%s): ",
        json_obj.dump(), absl::StrJoin(keys, ",")));
  } catch (const std::exception& e) {
    // The try-catch is to catch the exception thrown by the jwt-cpp
    // library. This is probably redundant since we already have the try-catch
    // in DecodeJwt, which is the only place where I suspect the exception can
    // throw. We add this outer try-catch to make the code more robust
    // and prevent exception leak into google3.
    return absl::InternalError(absl::StrFormat(
        "Failed to get value of key (%s) from token %s: due to exception: %s",
        absl::StrJoin(keys, ","), token, e.what()));
  }
}

}  // namespace agent_communication
