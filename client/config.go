// Copyright 2023 The xline Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import "time"

type ClientConfig struct {
	// Curp client wait sync timeout
	WaitSyncedTimeout time.Duration

	// Curp client propose request timeout
	ProposeTimeout time.Duration

	// Curp client initial retry interval
	InitialRetryTimeout time.Duration

	// Curp client max retry interval
	MaxRetryTimeout time.Duration

	// Curp client retry interval
	RetryCount int

	// Whether to use exponential backoff in retries
	UseBackoff *bool
}

const DefaultWaitSyncedTimeout = 2 * time.Second
const DefaultProposeTimeout = 1 * time.Second
const DefaultInitialRetryTimeout = 50 * time.Millisecond
const DefaultMaxRetryTimeout = 10000 * time.Millisecond
const DefaultRetryCount = 3

var t bool = true
var DefaultUseBackoff = &t

func newClientConfig(config *ClientConfig) {
	if config.WaitSyncedTimeout == 0 {
		config.WaitSyncedTimeout = DefaultWaitSyncedTimeout
	}
	if config.ProposeTimeout == 0 {
		config.ProposeTimeout = DefaultProposeTimeout
	}
	if config.InitialRetryTimeout == 0 {
		config.InitialRetryTimeout = DefaultInitialRetryTimeout
	}
	if config.MaxRetryTimeout == 0 {
		config.MaxRetryTimeout = DefaultMaxRetryTimeout
	}
	if config.RetryCount == 0 {
		config.RetryCount = DefaultRetryCount
	}
	if config.UseBackoff == nil {
		config.UseBackoff = DefaultUseBackoff
	}
}
