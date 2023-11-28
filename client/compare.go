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

import "github.com/xline-kv/go-xline/api/xline"

type CompareTarget int
type CompareResult int

const (
	CompareVersion CompareTarget = iota
	CompareCreated
	CompareModified
	CompareValue
)

type Cmp xlineapi.Compare

// nolint: govet
func Compare(cmp Cmp, result string, v interface{}) Cmp {
	var r xlineapi.Compare_CompareResult
	switch result {
	case "=":
		r = xlineapi.Compare_EQUAL
	case "!=":
		r = xlineapi.Compare_NOT_EQUAL
	case ">":
		r = xlineapi.Compare_GREATER
	case "<":
		r = xlineapi.Compare_LESS
	default:
		panic("Unknown result op")
	}
	cmp.Result = r

	switch cmp.Target {
	case xlineapi.Compare_VALUE:
		val, ok := v.(string)
		if !ok {
			panic("bad compare value")
		}
		cmp.TargetUnion = &xlineapi.Compare_Value{Value: []byte(val)}
	case xlineapi.Compare_VERSION:
		cmp.TargetUnion = &xlineapi.Compare_Version{Version: mustInt64(v)}
	case xlineapi.Compare_CREATE:
		cmp.TargetUnion = &xlineapi.Compare_CreateRevision{CreateRevision: mustInt64(v)}
	case xlineapi.Compare_MOD:
		cmp.TargetUnion = &xlineapi.Compare_ModRevision{ModRevision: mustInt64(v)}
	case xlineapi.Compare_LEASE:
		cmp.TargetUnion = &xlineapi.Compare_Lease{Lease: mustInt64(v)}
	default:
		panic("Unknown compare type")
	}
	return cmp
}

func Value(key []byte) Cmp {
	return Cmp{Key: []byte(key), Target: xlineapi.Compare_VALUE}
}

func Version(key []byte) Cmp {
	return Cmp{Key: []byte(key), Target: xlineapi.Compare_VERSION}
}

func CreateRevision(key []byte) Cmp {
	return Cmp{Key: []byte(key), Target: xlineapi.Compare_CREATE}
}

func ModRevision(key []byte) Cmp {
	return Cmp{Key: []byte(key), Target: xlineapi.Compare_MOD}
}

// LeaseValue compares a key's LeaseID to a value of your choosing. The empty
// LeaseID is 0, otherwise known as `NoLease`.
func LeaseValue(key []byte) Cmp {
	return Cmp{Key: []byte(key), Target: xlineapi.Compare_LEASE}
}

// nolint: govet
// WithRange sets the comparison to scan the range [key, end).
func (cmp Cmp) WithRange(end []byte) Cmp {
	cmp.RangeEnd = []byte(end)
	return cmp
}

// nolint: govet
// WithPrefix sets the comparison to scan all keys prefixed by the key.
func (cmp Cmp) WithPrefix() Cmp {
	cmp.RangeEnd = getPrefix(cmp.Key)
	return cmp
}

// mustInt64 panics if val isn't an int or int64. It returns an int64 otherwise.
func mustInt64(val interface{}) int64 {
	if v, ok := val.(int64); ok {
		return v
	}
	if v, ok := val.(int); ok {
		return int64(v)
	}
	panic("bad compare value")
}
