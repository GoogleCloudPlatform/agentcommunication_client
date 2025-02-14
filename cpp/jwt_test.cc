#include "cpp/jwt.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

// In external googletest, ASSERT_OK is not defined.
#ifndef ASSERT_OK
#define ASSERT_OK(x) ASSERT_TRUE(x.ok());
#endif

using ::testing::HasSubstr;

namespace agent_communication {
namespace {

constexpr absl::string_view kFullValidToken =
    "eyJhbGciOiJSUzI1NiIsImtpZCI6ImVlYzUzNGZhNWI4Y2FjYTIwMWNhOGQwZmY5NmI1NGM1Nj"
    "IyMTBkMWUiLCJ0eXAiOiJKV1QifQ."
    "eyJhdWQiOiJhZ2VudGNvbW11bmljYXRpb24uZ29vZ2xlYXBpcy5jb20iLCJhenAiOiIxMDk0Mj"
    "IyMzM4NzU4OTU4MjA1ODUiLCJlbWFpbCI6IjMyNjY5NDQzMzYxOC1jb21wdXRlQGRldmVsb3Bl"
    "ci5nc2VydmljZWFjY291bnQuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImV4cCI6MTczOT"
    "MyODM5MSwiZ29vZ2xlIjp7ImNvbXB1dGVfZW5naW5lIjp7Imluc3RhbmNlX2NyZWF0aW9uX3Rp"
    "bWVzdGFtcCI6MTczNDM3NzExNSwiaW5zdGFuY2VfaWQiOiIyODc1OTQ5NDg1MDc0MDA2MTM4Ii"
    "wiaW5zdGFuY2VfbmFtZSI6IndpLXRlc3Qtdm0tMSIsInByb2plY3RfaWQiOiJ3ZWlwZW5ncGFu"
    "ZGEtcHJvamVjdCIsInByb2plY3RfbnVtYmVyIjozMjY2OTQ0MzM2MTgsInpvbmUiOiJldXJvcG"
    "Utd2VzdDEtYiJ9fSwiaWF0IjoxNzM5MzI0NzkxLCJpc3MiOiJodHRwczovL2FjY291bnRzLmdv"
    "b2dsZS5jb20iLCJzdWIiOiIxMDk0MjIyMzM4NzU4OTU4MjA1ODUifQ.f93Oh-YLlafY_"
    "c8IBqXGnAR7O4LYtiT9eHdgnVwYlg-fmOCINz2FISMhFmYPX_rLLY1UIk-"
    "68iv2pCpyDi8dWugEB60fdObdPFPZLGjSTB7vFP5SBRLDFY_"
    "XOAHybHZfDnRTGujOzU0XFUebprHdcu1UJhSPpfAmudFwI7JWdiUG8wlDOqFkWFUasKJ0DcwjF"
    "QvkeQ-9QsiBMW9ZCbITUCU7klFoPYiFe-mTmRwxUZJFvFL-dmIyPAytgDa2_"
    "2uStApx617UPGgZlXLl3Zi6Xej_"
    "aZ9rWiWWQfkt2SnJmfEXzJYii5TPakvjvK8yVkI1GdCf7yD5HKCaFmBUzg_7rg";
// The payload of the above token as a reference:
/*
constexpr absl::string_view kFullValidTokenPayload = R"(
{
  "aud": "agentcommunication.googleapis.com",
  "azp": "109422233875895820585",
  "email": "326694433618-compute@developer.gserviceaccount.com",
  "email_verified": true,
  "exp": 1739328391,
  "google": {
    "compute_engine": {
      "instance_creation_timestamp": 1734377115,
      "instance_id": "2875949485074006138",
      "instance_name": "wi-test-vm-1",
      "project_id": "weipengpanda-project",
      "project_number": 326694433618,
      "zone": "europe-west1-b"
    }
  },
  "iat": 1739324791,
  "iss": "https://accounts.google.com",
  "sub": "109422233875895820585"
})";
*/

constexpr absl::string_view kInvalidToken = "invalid_token";

TEST(JwtTest, CanGetValuesFromTokenPayloadWithOneLayerKey) {
  // Test one layer query successfully.
  absl::StatusOr<std::string> audience =
      GetValueFromTokenPayloadWithKeys(std::string(kFullValidToken), {"aud"});
  ASSERT_OK(audience);
  EXPECT_EQ(*audience, "agentcommunication.googleapis.com");
}

TEST(JwtTest, FailureStatusWhenGetValuesFromTokenPayloadWithKeyMissing) {
  // Test one layer query unsuccessfully due to key missing.
  absl::StatusOr<std::string> invalid_key = GetValueFromTokenPayloadWithKeys(
      std::string(kFullValidToken), {"invalid_key"});
  ASSERT_FALSE(invalid_key.ok());
  EXPECT_THAT(invalid_key.status().code(), absl::StatusCode::kInternal);
  EXPECT_THAT(invalid_key.status().message(),
              HasSubstr("does not contain key: invalid_key"));
}

TEST(JwtTest, CanGetValuesFromTokenPayloadWithMultipleKey) {
  // Test multi-layer query successfully.
  absl::StatusOr<std::string> zone = GetValueFromTokenPayloadWithKeys(
      std::string(kFullValidToken), {"google", "compute_engine", "zone"});
  ASSERT_OK(zone);
  EXPECT_EQ(*zone, "europe-west1-b");
}

TEST(JwtTest, CanGetValuesFromTokenPayloadWithMultipleKeyWithNumberValue) {
  // Test multi-layer query successfully.
  absl::StatusOr<std::string> project_number = GetValueFromTokenPayloadWithKeys(
      std::string(kFullValidToken),
      {"google", "compute_engine", "project_number"});
  ASSERT_OK(project_number);
  EXPECT_EQ(*project_number, "326694433618");
}

TEST(JwtTest,
     FailureStatusWhenGetValuesFromTokenPayloadWithMultiLayerKeyMissing) {
  // Test multi-layer query unsuccessfully due to key missing.
  absl::StatusOr<std::string> invalid_key_in_middle =
      GetValueFromTokenPayloadWithKeys(
          std::string(kFullValidToken),
          {"google", "compute_engine", "invalid_key", "zone"});
  ASSERT_FALSE(invalid_key_in_middle.ok());
  EXPECT_THAT(invalid_key_in_middle.status().code(),
              absl::StatusCode::kInternal);
  EXPECT_THAT(invalid_key_in_middle.status().message(),
              HasSubstr("does not contain key: invalid_key"));
}

TEST(JwtTest,
     FailureStatusWhenGetValuesFromTokenPayloadWithKeyHasNonStringValue) {
  // Test multi-layer query unsuccessfully due to key has non string value.
  absl::StatusOr<std::string> google = GetValueFromTokenPayloadWithKeys(
      std::string(kFullValidToken), {"google"});
  ASSERT_FALSE(google.ok());
  EXPECT_THAT(google.status().code(), absl::StatusCode::kInternal);
  EXPECT_THAT(
      google.status().message(),
      HasSubstr("does not have a string/number value for key (google)"));
}

TEST(JwtTest, FailureStatusWhenGetValuesFromTokenPayloadWithTooManyLayersKey) {
  // Test multi-layer query unsuccessfully due to too many layers are queried.
  absl::StatusOr<std::string> zone_in_google = GetValueFromTokenPayloadWithKeys(
      std::string(kFullValidToken),
      {"google", "compute_engine", "zone", "invalid_key"});
  ASSERT_FALSE(zone_in_google.ok());
  EXPECT_THAT(zone_in_google.status().code(), absl::StatusCode::kInternal);
  EXPECT_THAT(zone_in_google.status().message(),
              HasSubstr("Token has an invalid format:"));
}

TEST(JwtTest, EmptyKeysInput) {
  absl::StatusOr<std::string> empty_keys =
      GetValueFromTokenPayloadWithKeys(std::string(kInvalidToken), {});
  ASSERT_FALSE(empty_keys.ok());
  EXPECT_THAT(empty_keys.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(empty_keys.status().message(),
              HasSubstr("Keys/token cannot be empty."));
}

TEST(JwtTest, EmptyTokenInput) {
  absl::StatusOr<std::string> empty_token =
      GetValueFromTokenPayloadWithKeys("", {"aud"});
  ASSERT_FALSE(empty_token.ok());
  EXPECT_THAT(empty_token.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(empty_token.status().message(),
              HasSubstr("Keys/token cannot be empty."));
}

TEST(JwtTest,
     FailureStatusWhenGetValuesFromTokenPayloadWithKeysFromInvalidToken) {
  absl::StatusOr<std::string> audience =
      GetValueFromTokenPayloadWithKeys(std::string(kInvalidToken), {"aud"});
  ASSERT_FALSE(audience.ok());
  EXPECT_THAT(audience.status().code(), absl::StatusCode::kInternal);
  EXPECT_THAT(audience.status().message(),
              HasSubstr("Failed to decode token: "));
}

}  // namespace
}  // namespace agent_communication
