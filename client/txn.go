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
	"fmt"
	"sync"

	"github.com/google/uuid"
	pb "github.com/xline-kv/go-xline/api/xline"
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
	// Name of the kv client, which will be used in CURP propose id generation
	name string
	// The client running the CURP protocol, communicate with all servers.
	curpClient curpClient
	// The auth token
	token string

	mu    sync.Mutex
	cif   bool
	cthen bool
	celse bool

	cmps []*pb.Compare
	sus  []*pb.RequestOp
	fas  []*pb.RequestOp
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
		txn.cmps = append(txn.cmps, (*pb.Compare)(&cs[i]))
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

	var krs []*pb.KeyRange
	for _, cmp := range txn.cmps {
		kr := &pb.KeyRange{Key: cmp.Key, RangeEnd: cmp.RangeEnd}
		krs = append(krs, kr)
	}
	req := &pb.TxnRequest{Compare: txn.cmps, Success: txn.sus, Failure: txn.fas}
	requestWithToken := pb.RequestWithToken{
		Token: &txn.token,
		RequestWrapper: &pb.RequestWithToken_TxnRequest{
			TxnRequest: req,
		},
	}
	proposeId := txn.generateProposeId()
	cmd := pb.Command{Keys: krs, Request: &requestWithToken, ProposeId: proposeId}
	res, err := txn.curpClient.propose(&cmd, false)
	if err != nil {
		return nil, err
	}
	return (*TxnResponse)(res.CommandResp.GetTxnResponse()), err
}

// TODO: update the propose id
// FYI: https://github.com/xline-kv/Xline/blob/84c685ac4b311ec035076b295e192c65644f85b9/curp-external-api/src/cmd.rs#L84
// Generate a new `ProposeId`
func (txn *txn) generateProposeId() string {
	return fmt.Sprintf("%s-%s", txn.name, uuid.New().String())
}
