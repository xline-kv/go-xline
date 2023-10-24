package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xline-kv/go-xline/client"
	"github.com/xline-kv/go-xline/xlog"
	"go.uber.org/zap/zapcore"
)

func TestPut(t *testing.T) {
	xlog.SetLevel(zapcore.WarnLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	xlineClient, _ := client.Connect(curpMembers)
	kvClient := xlineClient.Kv

	kvClient.Put(context.Background(), "put", "123")

	t.Run("overwrite_with_prev_key", func(t *testing.T) {
		res, _ := kvClient.Put(context.Background(), "put", "456", client.WithPutPrevKV())
		assert.Equal(t, "put", string(res.PrevKv.Key))
		assert.Equal(t, "123", string(res.PrevKv.Value))
	})

	t.Run("overwrite_again_with_prev_key", func(t *testing.T) {
		res, _ := kvClient.Put(context.Background(), "put", "456", client.WithPutPrevKV())
		assert.Equal(t, "put", string(res.PrevKv.Key))
		assert.Equal(t, "456", string(res.PrevKv.Value))
	})
}

func TestGet(t *testing.T) {
	xlog.SetLevel(zapcore.WarnLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	xlineClient, _ := client.Connect(curpMembers)
	kvClient := xlineClient.Kv

	kvClient.Put(context.Background(), "get10", "10")
	kvClient.Put(context.Background(), "get11", "11")
	kvClient.Put(context.Background(), "get20", "20")
	kvClient.Put(context.Background(), "get21", "21")

	t.Run("get_Key", func(t *testing.T) {
		res, _ := kvClient.Get(context.Background(), "get11")
		assert.False(t, res.More)
		assert.Len(t, res.Kvs, 1)
		assert.Equal(t, "get11", string(res.Kvs[0].Key))
		assert.Equal(t, "11", string(res.Kvs[0].Value))
	})

	t.Run("get_from_key", func(t *testing.T) {
		res, _ := kvClient.Get(context.Background(), "get11", client.WithGetFromKey(), client.WithGetLimit(2))
		assert.True(t, res.More)
		assert.Len(t, res.Kvs, 2)
		assert.Equal(t, "get11", string(res.Kvs[0].Key))
		assert.Equal(t, "11", string(res.Kvs[0].Value))
		assert.Equal(t, "get20", string(res.Kvs[1].Key))
		assert.Equal(t, "20", string(res.Kvs[1].Value))
	})

	t.Run("get_prefix_keys", func(t *testing.T) {
		res, _ := kvClient.Get(context.Background(), "get1", client.WithGetPrefix())
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
	xlog.SetLevel(zapcore.WarnLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	xlineClient, _ := client.Connect(curpMembers)
	kvClient := xlineClient.Kv

	kvClient.Put(context.Background(), "del10", "10")
	kvClient.Put(context.Background(), "del11", "11")
	kvClient.Put(context.Background(), "del20", "20")
	kvClient.Put(context.Background(), "del21", "21")
	kvClient.Put(context.Background(), "del31", "31")
	kvClient.Put(context.Background(), "del32", "32")

	t.Run("delete_key", func(t *testing.T) {
		delRes, _ := kvClient.Delete(context.Background(), "del11", client.WithDeletePrevKV())

		assert.Equal(t, delRes.Deleted, int64(1))
		assert.Equal(t, "del11", string(delRes.PrevKvs[0].Key))
		assert.Equal(t, "11", string(delRes.PrevKvs[0].Value))

		getRes, _ := kvClient.Get(context.Background(), "del11")
		assert.Equal(t, getRes.Count, int64(0))
	})

	t.Run("delete_a_range_of_keys", func(t *testing.T) {
		delRes, _ := kvClient.Delete(context.Background(), "del11", client.WithDeleteRange("del22"), client.WithDeletePrevKV())
		assert.Equal(t, int64(2), delRes.Deleted)
		assert.Equal(t, "del20", string(delRes.PrevKvs[0].Key))
		assert.Equal(t, "20", string(delRes.PrevKvs[0].Value))
		assert.Equal(t, "del21", string(delRes.PrevKvs[1].Key))
		assert.Equal(t, "21", string(delRes.PrevKvs[1].Value))

		getRes, _ := kvClient.Get(context.Background(), "del11")
		assert.Equal(t, int64(0), getRes.Count)
	})
}

// func TestTxn(t *testing.T) {
// 	xlog.SetLevel(zapcore.WarnLevel)

// 	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

// 	client, err := client.Connect(curpMembers)
// 	assert.NoError(t, err)
// 	kvClient := client.Kv

// 	_, err = kvClient.Put(&xlineapi.PutRequest{
// 		Key:   []byte("txn01"),
// 		Value: []byte("01"),
// 	})
// 	assert.NoError(t, err)

// 	t.Run("transaction_1", func(t *testing.T) {
// 		txnRes, err := kvClient.Txn(&xlineapi.TxnRequest{
// 			Compare: []*xlineapi.Compare{
// 				{
// 					Key:         []byte("txn01"),
// 					TargetUnion: &xlineapi.Compare_Value{Value: []byte("01")},
// 					Result:      xlineapi.Compare_EQUAL,
// 					Target:      xlineapi.Compare_VALUE,
// 				},
// 			},
// 			Success: []*xlineapi.RequestOp{
// 				{
// 					Request: &xlineapi.RequestOp_RequestPut{
// 						RequestPut: &xlineapi.PutRequest{
// 							Key:    []byte("txn01"),
// 							Value:  []byte("02"),
// 							PrevKv: true,
// 						},
// 					},
// 				},
// 			},
// 			Failure: []*xlineapi.RequestOp{
// 				{
// 					Request: &xlineapi.RequestOp_RequestRange{
// 						RequestRange: &xlineapi.RangeRequest{
// 							Key: []byte("txn01"),
// 						},
// 					},
// 				},
// 			},
// 		})

// 		assert.NoError(t, err)
// 		assert.True(t, txnRes.Succeeded)
// 		opRes := txnRes.Responses
// 		assert.Len(t, opRes, 1)
// 		putRes := opRes[0].GetResponsePut()
// 		assert.NotNil(t, putRes)
// 		assert.Equal(t, "01", string(putRes.PrevKv.Value))

// 		rangeRes, err := kvClient.Range(&xlineapi.RangeRequest{
// 			Key: []byte("txn01"),
// 		})

// 		assert.NoError(t, err)
// 		assert.Equal(t, "txn01", string(rangeRes.Kvs[0].Key))
// 		assert.Equal(t, "02", string(rangeRes.Kvs[0].Value))
// 	})

// 	t.Run("transaction_2", func(t *testing.T) {
// 		txnRes, err := kvClient.Txn(&xlineapi.TxnRequest{
// 			Compare: []*xlineapi.Compare{
// 				{
// 					Key:         []byte("txn01"),
// 					TargetUnion: &xlineapi.Compare_Value{Value: []byte("01")},
// 					Result:      xlineapi.Compare_EQUAL,
// 					Target:      xlineapi.Compare_VALUE,
// 				},
// 			},
// 			Success: []*xlineapi.RequestOp{
// 				{
// 					Request: &xlineapi.RequestOp_RequestPut{
// 						RequestPut: &xlineapi.PutRequest{
// 							Key:    []byte("txn01"),
// 							Value:  []byte("02"),
// 							PrevKv: true,
// 						},
// 					},
// 				},
// 			},
// 			Failure: []*xlineapi.RequestOp{
// 				{
// 					Request: &xlineapi.RequestOp_RequestRange{
// 						RequestRange: &xlineapi.RangeRequest{
// 							Key: []byte("txn01"),
// 						},
// 					},
// 				},
// 			},
// 		})

// 		assert.NoError(t, err)
// 		assert.False(t, txnRes.Succeeded)
// 		opRes := txnRes.Responses
// 		assert.Len(t, opRes, 1)
// 		rangeRes := opRes[0].GetResponseRange()
// 		assert.NotNil(t, rangeRes)
// 		assert.Equal(t, "02", string(rangeRes.Kvs[0].Value))
// 	})
// }

func TestCompact(t *testing.T) {
	xlog.SetLevel(zapcore.WarnLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	xlineClient, _ := client.Connect(curpMembers)
	kvClient := xlineClient.Kv

	kvClient.Put(context.Background(), "compact", "0")
	putRes, _ := kvClient.Put(context.Background(), "compact", "1")
	rev := putRes.Header.Revision

	// before compacting
	rev0res, _ := kvClient.Get(context.Background(), "compact", client.WithGetRev(rev-1))
	assert.NotNil(t, rev0res)
	assert.Equal(t, "0", string(rev0res.Kvs[0].Value))

	rev1res, _ := kvClient.Get(context.Background(), "compact", client.WithGetRev(rev))
	assert.NotNil(t, rev1res)
	assert.Equal(t, "1", string(rev1res.Kvs[0].Value))

	kvClient.Compact(context.Background(), rev)

	// after compacting
	_, err := kvClient.Get(context.Background(), "compact", client.WithGetRev(rev-1))
	assert.Error(t, err)

	getRes, _ := kvClient.Get(context.Background(), "compact", client.WithGetRev(rev))
	assert.Equal(t, "1", string(getRes.Kvs[0].Value))
}
