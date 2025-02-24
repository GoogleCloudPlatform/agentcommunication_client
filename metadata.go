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

type cachedValue struct {
	k      string
	v      string
	parser func(string) string
	sync.Mutex
}

var (
	zone    = &cachedValue{k: "instance/zone", parser: func(v string) string { return v[strings.LastIndex(v, "/")+1:] }}
	projNum = &cachedValue{k: "project/numeric-project-id"}
	instID  = &cachedValue{k: "instance/id"}
	idToken = &cachedIDToken{}
)

func (c *cachedValue) get() (string, error) {
	c.Lock()
	defer c.Unlock()
	if c.v != "" {
		return c.v, nil
	}
	var err error
	c.v, err = metadata.Get(c.k)
	if err != nil {
		return "", err
	}
	if c.parser != nil {
		c.v = c.parser(c.v)
	}
	return c.v, nil
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
	expTime *time.Time
	raw     string
	sync.Mutex
}

func (t *cachedIDToken) get() error {
	data, err := metadata.Get(identityTokenPath)
	if err != nil {
		return fmt.Errorf("error getting token from metadata: %w", err)
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
	idToken.Lock()
	defer idToken.Unlock()

	// Re-request token if expiry is within 10 minutes.
	if idToken.expTime == nil || time.Now().After(idToken.expTime.Add(-10*time.Minute)) {
		if err := idToken.get(); err != nil {
			return "", err
		}
	}

	return idToken.raw, nil
}

func getZone() (string, error) {
	return zone.get()
}

func getProjectNumber() (string, error) {
	return projNum.get()
}

func getInstanceID() (string, error) {
	return instID.get()
}

func getResourceID() (string, error) {
	zone, err := getZone()
	if err != nil {
		return "", err
	}
	projectNum, err := getProjectNumber()
	if err != nil {
		return "", err
	}
	instanceID, err := getInstanceID()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("projects/%s/zones/%s/instances/%s", projectNum, zone, instanceID), nil
}
