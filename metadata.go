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
	"context"
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
	identityTokenPath     = "instance/service-accounts/default/identity?audience=agentcommunication.googleapis.com&format=full"
	defaultUniverseDomain = "googleapis.com"
)

var (
	metadataInited bool
	metadataInitMx sync.RWMutex
	metadataInit   = func(ctx context.Context) error {
		metadataInitMx.Lock()
		defer metadataInitMx.Unlock()
		if metadataInited {
			return nil
		}

		metadataInitData, err := MetadataInitFunc(ctx)
		if err != nil {
			loggerPrintf("Failed to initialize metadata: %v", err)
			return err
		}
		if metadataInitData == nil {
			return errors.New("metadata init data is nil")
		}

		protectedZone = metadataInitData.Zone
		protectedResourceID = metadataInitData.ResourceID
		protectedIDToken = &cachedIDToken{tokenGetter: metadataInitData.TokenGetter}
		protectedUniverseDomain = metadataInitData.UniverseDomain

		metadataInited = true
		return nil
	}
	// MetadataInitFunc is a function that initializes the metadata. If not set, the metadata will be
	// initialized for GCE.
	MetadataInitFunc func(context.Context) (*MetadataInitData, error) = initGCEMetadata

	protectedZone           string
	protectedResourceID     string
	protectedUniverseDomain string
	protectedIDToken        = &cachedIDToken{}
)

// MetadataInitData contains the data needed to initialize the metadata. This is returned by the
// MetadataInitFunc.
type MetadataInitData struct {
	Zone           string
	ResourceID     string
	UniverseDomain string
	TokenGetter    func() (string, error)
}

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

func getUniverseDomain() string {
	metadataInitMx.RLock()
	defer metadataInitMx.RUnlock()
	return protectedUniverseDomain
}

func initGCEMetadata(ctx context.Context) (*MetadataInitData, error) {
	loggerPrintf("Running in GCE")
	zone, err := getGCEZone(ctx)
	if err != nil {
		return nil, err
	}
	resourceID, err := getGCEResourceID(ctx, zone)
	if err != nil {
		return nil, err
	}
	universeDomain, err := getGCEUniverseDomain(ctx)
	if err != nil {
		return nil, err
	}

	metadataInitData := &MetadataInitData{
		Zone:           zone,
		ResourceID:     resourceID,
		UniverseDomain: universeDomain,
		TokenGetter:    func() (string, error) { return metadata.GetWithContext(ctx, identityTokenPath) },
	}

	return metadataInitData, nil
}

func getGCEZone(ctx context.Context) (string, error) {
	zone, err := metadata.GetWithContext(ctx, "instance/zone")
	if err != nil {
		return "", err
	}

	return zone[strings.LastIndex(zone, "/")+1:], nil
}

func getGCEResourceID(ctx context.Context, zone string) (string, error) {
	projectNum, err := metadata.GetWithContext(ctx, "project/numeric-project-id")
	if err != nil {
		return "", err
	}
	instanceID, err := metadata.GetWithContext(ctx, "instance/id")
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("projects/%s/zones/%s/instances/%s", projectNum, zone, instanceID), nil
}

func getGCEUniverseDomain(ctx context.Context) (string, error) {
	universeDomain, err := metadata.GetWithContext(ctx, "universe/universe-domain")
	// For now fail open if the universe domain is not set, this should be moved to a checking the
	// HTTP response in the future (only fail open on 404).
	if err != nil || universeDomain == "" {
		loggerPrintf("Universe domain is not set, using googleapis.com")
		universeDomain = defaultUniverseDomain
	}

	// Fail if the universe domain is set to something other than googleapis.com
	if universeDomain != defaultUniverseDomain {
		return "", &ErrUnsupportedUniverse{universe: universeDomain}
	}

	loggerPrintf("Universe domain is set to %q", universeDomain)
	return universeDomain, nil
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
