// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/compute/metadata"
)

const (
	identityTokenPath = "instance/service-accounts/default/identity?audience=agentcommunication.googleapis.com&format=full"
)

var (
	metadataInited bool
	metadataInitMx sync.RWMutex
	metadataInit   = func() error {
		metadataInitMx.Lock()
		defer metadataInitMx.Unlock()
		if metadataInited {
			return nil
		}

		z, r, tokenGetter, err := MetadataInitFunc()
		if err != nil {
			loggerPrintf("Failed to initialize metadata: %v", err)
			return err
		}
		protectedZone = z
		protectedResourceID = r
		protectedIDToken = &cachedIDToken{tokenGetter: tokenGetter}

		metadataInited = true
		return nil
	}
	// MetadataInitFunc is a function that initializes the metadata. If not set, the metadata will be
	// initialized for GCE.
	MetadataInitFunc func() (string, string, func() (string, error), error) = initGCEMetadata

	protectedZone       string
	protectedResourceID string
	protectedIDToken    = &cachedIDToken{}
)

func getResourceID() string {
	metadataInitMx.RLock()
	defer metadataInitMx.RUnlock()
	return protectedResourceID
}

func getZone() string {
	metadataInitMx.RLock()
	defer metadataInitMx.RUnlock()
	return protectedZone
}

func initGCEMetadata() (string, string, func() (string, error), error) {
	loggerPrintf("Running in GCE")
	zone, err := getGCEZone()
	if err != nil {
		return "", "", nil, err
	}
	resourceID, err := getGCEResourceID(zone)
	if err != nil {
		return "", "", nil, err
	}
	tokenGetter := func() (string, error) { return metadata.Get(identityTokenPath) }

	return zone, resourceID, tokenGetter, nil
}

func getGCEZone() (string, error) {
	zone, err := metadata.Get("instance/zone")
	if err != nil {
		return "", err
	}

	return zone[strings.LastIndex(zone, "/")+1:], nil
}

func getGCEResourceID(zone string) (string, error) {
	projectNum, err := metadata.Get("project/numeric-project-id")
	if err != nil {
		return "", err
	}
	instanceID, err := metadata.Get("instance/id")
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("projects/%s/zones/%s/instances/%s", projectNum, zone, instanceID), nil
}

type claimSet struct {
	Exp int64 `json:"exp"` // this is all we are interested in
}

func decodeTokenExpiry(payload string) (int64, error) {
	// decode returned id token to get expiry
	s := strings.Split(payload, ".")
	if len(s) < 2 {
		return 0, errors.New("invalid token received")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(s[1])
	if err != nil {
		return 0, err
	}
	c := &claimSet{}
	err = json.NewDecoder(bytes.NewBuffer(decoded)).Decode(c)
	return c.Exp, err
}

type cachedIDToken struct {
	expTime     *time.Time
	raw         string
	tokenGetter func() (string, error)
	sync.Mutex
}

func (t *cachedIDToken) get() error {
	if t.tokenGetter == nil {
		return errors.New("no token getter set")
	}
	data, err := t.tokenGetter()
	if err != nil {
		return err
	}

	exp, err := decodeTokenExpiry(data)
	if err != nil {
		return err
	}

	t.raw = data
	expTime := time.Unix(exp, 0)
	t.expTime = &expTime

	return nil
}

func getIdentityToken() (string, error) {
	metadataInitMx.RLock()
	defer metadataInitMx.RUnlock()
	protectedIDToken.Lock()
	defer protectedIDToken.Unlock()

	// Re-request token if expiry is within 10 minutes.
	if protectedIDToken.expTime == nil || time.Now().After(protectedIDToken.expTime.Add(-10*time.Minute)) {
		if err := protectedIDToken.get(); err != nil {
			return "", err
		}
	}

	return protectedIDToken.raw, nil
}
