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

package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xline-kv/go-xline/client"
	"github.com/xline-kv/go-xline/xlog"
	"go.uber.org/zap/zapcore"
)

func TestPut(t *testing.T) {
	xlog.SetLevel(zapcore.ErrorLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	xlineClient, _ := client.Connect(curpMembers)
	kvClient := xlineClient.KV

	kvClient.Put([]byte("put"), []byte("123"))

	t.Run("overwrite_with_prev_key", func(t *testing.T) {
		res, _ := kvClient.Put([]byte("put"), []byte("456"), client.WithPrevKV())
		assert.Equal(t, "put", string(res.PrevKv.Key))
		assert.Equal(t, "123", string(res.PrevKv.Value))
	})

	t.Run("overwrite_again_with_prev_key", func(t *testing.T) {
		res, _ := kvClient.Put([]byte("put"), []byte("456"), client.WithPrevKV())
		assert.Equal(t, "put", string(res.PrevKv.Key))
		assert.Equal(t, "456", string(res.PrevKv.Value))
	})
}

func TestGet(t *testing.T) {
	xlog.SetLevel(zapcore.ErrorLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	xlineClient, _ := client.Connect(curpMembers)
	kvClient := xlineClient.KV

	kvClient.Put([]byte("get10"), []byte("10"))
	kvClient.Put([]byte("get11"), []byte("11"))
	kvClient.Put([]byte("get20"), []byte("20"))
	kvClient.Put([]byte("get21"), []byte("21"))

	t.Run("get_Key", func(t *testing.T) {
		res, _ := kvClient.Range([]byte("get11"))
		assert.False(t, res.More)
		assert.Len(t, res.Kvs, 1)
		assert.Equal(t, "get11", string(res.Kvs[0].Key))
		assert.Equal(t, "11", string(res.Kvs[0].Value))
	})

	t.Run("get_from_key", func(t *testing.T) {
		res, _ := kvClient.Range([]byte("get11"), client.WithFromKey(), client.WithLimit(2))
		assert.True(t, res.More)
		assert.Len(t, res.Kvs, 2)
		assert.Equal(t, "get11", string(res.Kvs[0].Key))
		assert.Equal(t, "11", string(res.Kvs[0].Value))
		assert.Equal(t, "get20", string(res.Kvs[1].Key))
		assert.Equal(t, "20", string(res.Kvs[1].Value))
	})

	t.Run("get_prefix_keys", func(t *testing.T) {
		res, _ := kvClient.Range([]byte("get1"), client.WithPrefix())
		assert.Equal(t, int64(2), res.Count)
		assert.False(t, res.More)
		assert.Len(t, res.Kvs, 2)
		assert.Equal(t, "get10", string(res.Kvs[0].Key))
		assert.Equal(t, "10", string(res.Kvs[0].Value))
		assert.Equal(t, "get11", string(res.Kvs[1].Key))
		assert.Equal(t, "11", string(res.Kvs[1].Value))
	})
}

func TestDelete(t *testing.T) {
	xlog.SetLevel(zapcore.ErrorLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	xlineClient, _ := client.Connect(curpMembers)
	kvClient := xlineClient.KV

	kvClient.Put([]byte("del10"), []byte("10"))
	kvClient.Put([]byte("del11"), []byte("11"))
	kvClient.Put([]byte("del20"), []byte("20"))
	kvClient.Put([]byte("del21"), []byte("21"))
	kvClient.Put([]byte("del31"), []byte("31"))
	kvClient.Put([]byte("del32"), []byte("32"))

	t.Run("delete_key", func(t *testing.T) {
		delRes, _ := kvClient.Delete("del11", client.WithPrevKV())

		assert.Equal(t, delRes.Deleted, int64(1))
		assert.Equal(t, "del11", string(delRes.PrevKvs[0].Key))
		assert.Equal(t, "11", string(delRes.PrevKvs[0].Value))

		getRes, _ := kvClient.Range([]byte("del11"), client.WithCountOnly())
		assert.Equal(t, getRes.Count, int64(0))
	})

	t.Run("delete_a_range_of_keys", func(t *testing.T) {
		delRes, _ := kvClient.Delete("del11", client.WithRange([]byte("del22")), client.WithPrevKV())
		assert.Equal(t, int64(2), delRes.Deleted)
		assert.Equal(t, "del20", string(delRes.PrevKvs[0].Key))
		assert.Equal(t, "20", string(delRes.PrevKvs[0].Value))
		assert.Equal(t, "del21", string(delRes.PrevKvs[1].Key))
		assert.Equal(t, "21", string(delRes.PrevKvs[1].Value))

		getRes, _ := kvClient.Range([]byte("del11"), client.WithCountOnly())
		assert.Equal(t, int64(0), getRes.Count)
	})

	t.Run("delete key with prefix", func(t *testing.T) {
		delRes, _ := kvClient.Delete("del3", client.WithPrefix(), client.WithPrevKV())
		assert.Equal(t, int64(2), delRes.Deleted)
		assert.Equal(t, "del31", string(delRes.PrevKvs[0].Key))
		assert.Equal(t, "31", string(delRes.PrevKvs[0].Value))
		assert.Equal(t, "del32", string(delRes.PrevKvs[1].Key))
		assert.Equal(t, "32", string(delRes.PrevKvs[1].Value))

		getRes, _ := kvClient.Range([]byte("del32"), client.WithCountOnly())
		assert.Equal(t, int64(0), getRes.Count)
	})
}

func TestTxn(t *testing.T) {
	xlog.SetLevel(zapcore.ErrorLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	xlineClient, _ := client.Connect(curpMembers)
	kvClient := xlineClient.KV

	kvClient.Put([]byte("txn01"), []byte("01"))

	t.Run("transaction_1", func(t *testing.T) {
		txnRes, _ := kvClient.Txn().
			When(client.Compare(client.Value([]byte("txn01")), "=", "01")).
			AndThen(client.OpPut([]byte("txn01"), []byte("02"), client.WithPrevKV())).
			OrElse(client.OpRange([]byte("txn01"))).
			Commit()

		assert.True(t, txnRes.Succeeded)
		opRes := txnRes.Responses
		assert.Len(t, opRes, 1)
		putRes := opRes[0].GetResponsePut()
		assert.NotNil(t, putRes)
		assert.Equal(t, "01", string(putRes.PrevKv.Value))

		getRes, _ := kvClient.Range([]byte("txn01"))
		assert.Equal(t, "txn01", string(getRes.Kvs[0].Key))
		assert.Equal(t, "02", string(getRes.Kvs[0].Value))
	})

	t.Run("transaction_2", func(t *testing.T) {
		txnRes, _ := kvClient.Txn().
			When(client.Compare(client.Value([]byte("txn01")), "=", "01")).
			AndThen(client.OpPut([]byte("txn01"), []byte("02"), client.WithPrevKV())).
			OrElse(client.OpRange([]byte("txn01"))).
			Commit()

		assert.False(t, txnRes.Succeeded)
		opRes := txnRes.Responses
		assert.Len(t, opRes, 1)
		rangeRes := opRes[0].GetResponseRange()
		assert.NotNil(t, rangeRes)
		assert.Equal(t, "02", string(rangeRes.Kvs[0].Value))
	})
}

func TestCompact(t *testing.T) {
	xlog.SetLevel(zapcore.ErrorLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	xlineClient, _ := client.Connect(curpMembers)
	kvClient := xlineClient.KV

	kvClient.Put([]byte("compact"), []byte("0"))
	putRes, _ := kvClient.Put([]byte("compact"), []byte("1"))
	rev := putRes.Header.Revision

	// before compacting
	rev0res, _ := kvClient.Range([]byte("compact"), client.WithRev(rev-1))
	assert.NotNil(t, rev0res)
	assert.Equal(t, "0", string(rev0res.Kvs[0].Value))

	rev1res, _ := kvClient.Range([]byte("compact"), client.WithRev(rev))
	assert.NotNil(t, rev1res)
	assert.Equal(t, "1", string(rev1res.Kvs[0].Value))

	kvClient.Compact(rev)

	// after compacting
	_, err := kvClient.Range([]byte("compact"), client.WithRev(rev-1))
	assert.Error(t, err)

	getRes, _ := kvClient.Range([]byte("compact"), client.WithRev(rev))
	assert.Equal(t, "1", string(getRes.Kvs[0].Value))
}
