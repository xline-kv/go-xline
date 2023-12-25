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

import (
	"sync"

	"github.com/xline-kv/go-xline/api/xline"
)

type Txn interface {
	// When takes a list of comparison. When all comparisons passed in succeed,
	// the operations passed into Then() will be executed. Or the operations
	// passed into Else() will be executed.
	When(cs ...Cmp) Txn

	// AndThen takes a list of operations. The Ops list will be executed, if the
	// comparisons passed in If() succeed.
	AndThen(ops ...Op) Txn

	// OrElse takes a list of operations. The Ops list will be executed, if the
	// comparisons passed in If() fail.
	OrElse(ops ...Op) Txn

	// Commit tries to commit the transaction.
	Commit() (*TxnResponse, error)
}

type txn struct {
	// The client running the CURP protocol, communicate with all servers.
	curpClient Curp
	// The auth token
	token string

	mu    sync.Mutex
	cif   bool
	cthen bool
	celse bool

	cmps []*xlineapi.Compare
	sus  []*xlineapi.RequestOp
	fas  []*xlineapi.RequestOp
}

func (txn *txn) When(cs ...Cmp) Txn {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.cif {
		panic("cannot call If twice!")
	}
	if txn.cthen {
		panic("cannot call If after Then!")
	}
	if txn.celse {
		panic("cannot call If after Else!")
	}

	txn.cif = true

	for i := range cs {
		txn.cmps = append(txn.cmps, (*xlineapi.Compare)(&cs[i]))
	}

	return txn
}

func (txn *txn) AndThen(ops ...Op) Txn {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.cthen {
		panic("cannot call Then twice!")
	}
	if txn.celse {
		panic("cannot call Then after Else!")
	}

	txn.cthen = true

	for _, op := range ops {
		txn.sus = append(txn.sus, op.toRequestOp())
	}

	return txn
}

func (txn *txn) OrElse(ops ...Op) Txn {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.celse {
		panic("cannot call Else twice!")
	}

	txn.celse = true

	for _, op := range ops {
		txn.fas = append(txn.fas, op.toRequestOp())
	}

	return txn
}

func (txn *txn) Commit() (*TxnResponse, error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	var krs []*xlineapi.KeyRange
	for _, cmp := range txn.cmps {
		kr := &xlineapi.KeyRange{Key: cmp.Key, RangeEnd: cmp.RangeEnd}
		krs = append(krs, kr)
	}
	req := &xlineapi.TxnRequest{Compare: txn.cmps, Success: txn.sus, Failure: txn.fas}
	requestWithToken := xlineapi.RequestWithToken{
		Token: &txn.token,
		RequestWrapper: &xlineapi.RequestWithToken_TxnRequest{
			TxnRequest: req,
		},
	}
	cmd := xlineapi.Command{Keys: krs, Request: &requestWithToken}
	res, err := txn.curpClient.Propose(&cmd, false)
	if err != nil {
		return nil, err
	}
	return (*TxnResponse)(res.Er.GetTxnResponse()), err
}
