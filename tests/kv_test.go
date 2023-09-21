package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	xlineapi "github.com/xline-kv/go-xline/api/xline"
	"github.com/xline-kv/go-xline/client"
	"github.com/xline-kv/go-xline/xlog"
	"go.uber.org/zap/zapcore"
)

func TestPut(t *testing.T) {
	xlog.SetLevel(zapcore.WarnLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	client, err := client.Connect(curpMembers)
	assert.NoError(t, err)
	kvClient := client.Kv

	_, err = kvClient.Put(&xlineapi.PutRequest{
		Key:   []byte("put"),
		Value: []byte("123"),
	})
	assert.NoError(t, err)

	t.Run("overwrite_with_prev_key", func(t *testing.T) {
		res, err := kvClient.Put(&xlineapi.PutRequest{
			Key:    []byte("put"),
			Value:  []byte("456"),
			PrevKv: true,
		})

		assert.NoError(t, err)
		assert.Equal(t, "put", string(res.PrevKv.Key))
		assert.Equal(t, "123", string(res.PrevKv.Value))
	})

	t.Run("overwrite_again_with_prev_key", func(t *testing.T) {
		res, err := kvClient.Put(&xlineapi.PutRequest{
			Key:    []byte("put"),
			Value:  []byte("456"),
			PrevKv: true,
		})

		assert.NoError(t, err)
		assert.Equal(t, "put", string(res.PrevKv.Key))
		assert.Equal(t, "456", string(res.PrevKv.Value))
	})
}

func TestRange(t *testing.T) {
	xlog.SetLevel(zapcore.WarnLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	client, err := client.Connect(curpMembers)
	assert.NoError(t, err)
	kvClient := client.Kv

	_, err = kvClient.Put(&xlineapi.PutRequest{
		Key:   []byte("get10"),
		Value: []byte("10"),
	})
	assert.NoError(t, err)
	_, err = kvClient.Put(&xlineapi.PutRequest{
		Key:   []byte("get11"),
		Value: []byte("11"),
	})
	assert.NoError(t, err)
	_, err = kvClient.Put(&xlineapi.PutRequest{
		Key:   []byte("get20"),
		Value: []byte("20"),
	})
	assert.NoError(t, err)
	_, err = kvClient.Put(&xlineapi.PutRequest{
		Key:   []byte("get21"),
		Value: []byte("21"),
	})
	assert.NoError(t, err)

	t.Run("get_Key", func(t *testing.T) {
		res, err := kvClient.Range(&xlineapi.RangeRequest{
			Key: []byte("get11"),
		})

		assert.NoError(t, err)
		assert.False(t, res.More)
		assert.Len(t, res.Kvs, 1)
		assert.Equal(t, "get11", string(res.Kvs[0].Key))
		assert.Equal(t, "11", string(res.Kvs[0].Value))
	})

	t.Run("get_from_key", func(t *testing.T) {
		res, err := kvClient.Range(&xlineapi.RangeRequest{
			Key:      []byte("get11"),
			RangeEnd: []byte{0},
			Limit:    2,
		})

		assert.NoError(t, err)
		assert.True(t, res.More)
		assert.Len(t, res.Kvs, 2)
		assert.Equal(t, "get11", string(res.Kvs[0].Key))
		assert.Equal(t, "11", string(res.Kvs[0].Value))
		assert.Equal(t, "get20", string(res.Kvs[1].Key))
		assert.Equal(t, "20", string(res.Kvs[1].Value))
	})

	t.Run("get_prefix_keys", func(t *testing.T) {
		res, err := kvClient.Range(&xlineapi.RangeRequest{
			Key:      []byte("get1"),
			RangeEnd: []byte("get2"),
		})

		assert.NoError(t, err)
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

	client, err := client.Connect(curpMembers)
	assert.NoError(t, err)
	kvClient := client.Kv

	_, err = kvClient.Put(&xlineapi.PutRequest{
		Key:   []byte("del10"),
		Value: []byte("10"),
	})
	assert.NoError(t, err)
	_, err = kvClient.Put(&xlineapi.PutRequest{
		Key:   []byte("del11"),
		Value: []byte("11"),
	})
	assert.NoError(t, err)
	_, err = kvClient.Put(&xlineapi.PutRequest{
		Key:   []byte("del20"),
		Value: []byte("20"),
	})
	assert.NoError(t, err)
	_, err = kvClient.Put(&xlineapi.PutRequest{
		Key:   []byte("del21"),
		Value: []byte("21"),
	})
	assert.NoError(t, err)
	_, err = kvClient.Put(&xlineapi.PutRequest{
		Key:   []byte("del31"),
		Value: []byte("31"),
	})
	assert.NoError(t, err)
	_, err = kvClient.Put(&xlineapi.PutRequest{
		Key:   []byte("del32"),
		Value: []byte("32"),
	})
	assert.NoError(t, err)

	t.Run("delete_key", func(t *testing.T) {
		delRes, err := kvClient.Delete(&xlineapi.DeleteRangeRequest{
			Key:    []byte("del11"),
			PrevKv: true,
		})

		assert.NoError(t, err)
		assert.Equal(t, delRes.Deleted, int64(1))
		assert.Equal(t, "del11", string(delRes.PrevKvs[0].Key))
		assert.Equal(t, "11", string(delRes.PrevKvs[0].Value))

		rangeRes, err := kvClient.Range(&xlineapi.RangeRequest{
			Key: []byte("del11"),
		})

		assert.NoError(t, err)
		assert.Equal(t, rangeRes.Count, int64(0))
	})

	t.Run("delete_a_range_of_keys", func(t *testing.T) {
		delRes, err := kvClient.Delete(&xlineapi.DeleteRangeRequest{
			Key:      []byte("del11"),
			RangeEnd: []byte("del22"),
			PrevKv:   true,
		})

		assert.NoError(t, err)
		assert.Equal(t, int64(2), delRes.Deleted)
		assert.Equal(t, "del20", string(delRes.PrevKvs[0].Key))
		assert.Equal(t, "20", string(delRes.PrevKvs[0].Value))
		assert.Equal(t, "del21", string(delRes.PrevKvs[1].Key))
		assert.Equal(t, "21", string(delRes.PrevKvs[1].Value))

		rangeRes, err := kvClient.Range(&xlineapi.RangeRequest{
			Key: []byte("del11"),
		})

		assert.NoError(t, err)
		assert.Equal(t, int64(0), rangeRes.Count)
	})
}

func TestTxn(t *testing.T) {
	xlog.SetLevel(zapcore.WarnLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	client, err := client.Connect(curpMembers)
	assert.NoError(t, err)
	kvClient := client.Kv

	_, err = kvClient.Put(&xlineapi.PutRequest{
		Key:   []byte("txn01"),
		Value: []byte("01"),
	})
	assert.NoError(t, err)

	t.Run("transaction_1", func(t *testing.T) {
		txnRes, err := kvClient.Txn(&xlineapi.TxnRequest{
			Compare: []*xlineapi.Compare{
				{
					Key:         []byte("txn01"),
					TargetUnion: &xlineapi.Compare_Value{Value: []byte("01")},
					Result:      xlineapi.Compare_EQUAL,
					Target:      xlineapi.Compare_VALUE,
				},
			},
			Success: []*xlineapi.RequestOp{
				{
					Request: &xlineapi.RequestOp_RequestPut{
						RequestPut: &xlineapi.PutRequest{
							Key:    []byte("txn01"),
							Value:  []byte("02"),
							PrevKv: true,
						},
					},
				},
			},
			Failure: []*xlineapi.RequestOp{
				{
					Request: &xlineapi.RequestOp_RequestRange{
						RequestRange: &xlineapi.RangeRequest{
							Key: []byte("txn01"),
						},
					},
				},
			},
		})

		assert.NoError(t, err)
		assert.True(t, txnRes.Succeeded)
		opRes := txnRes.Responses
		assert.Len(t, opRes, 1)
		putRes := opRes[0].GetResponsePut()
		assert.NotNil(t, putRes)
		assert.Equal(t, "01", string(putRes.PrevKv.Value))

		rangeRes, err := kvClient.Range(&xlineapi.RangeRequest{
			Key: []byte("txn01"),
		})

		assert.NoError(t, err)
		assert.Equal(t, "txn01", string(rangeRes.Kvs[0].Key))
		assert.Equal(t, "02", string(rangeRes.Kvs[0].Value))
	})

	t.Run("transaction_2", func(t *testing.T) {
		txnRes, err := kvClient.Txn(&xlineapi.TxnRequest{
			Compare: []*xlineapi.Compare{
				{
					Key:         []byte("txn01"),
					TargetUnion: &xlineapi.Compare_Value{Value: []byte("01")},
					Result:      xlineapi.Compare_EQUAL,
					Target:      xlineapi.Compare_VALUE,
				},
			},
			Success: []*xlineapi.RequestOp{
				{
					Request: &xlineapi.RequestOp_RequestPut{
						RequestPut: &xlineapi.PutRequest{
							Key:    []byte("txn01"),
							Value:  []byte("02"),
							PrevKv: true,
						},
					},
				},
			},
			Failure: []*xlineapi.RequestOp{
				{
					Request: &xlineapi.RequestOp_RequestRange{
						RequestRange: &xlineapi.RangeRequest{
							Key: []byte("txn01"),
						},
					},
				},
			},
		})

		assert.NoError(t, err)
		assert.False(t, txnRes.Succeeded)
		opRes := txnRes.Responses
		assert.Len(t, opRes, 1)
		rangeRes := opRes[0].GetResponseRange()
		assert.NotNil(t, rangeRes)
		assert.Equal(t, "02", string(rangeRes.Kvs[0].Value))
	})
}

func TestCompact(t *testing.T) {
	xlog.SetLevel(zapcore.WarnLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	client, err := client.Connect(curpMembers)
	assert.NoError(t, err)
	kvClient := client.Kv

	_, err = kvClient.Put(&xlineapi.PutRequest{
		Key:   []byte("compact"),
		Value: []byte("0"),
	})
	assert.NoError(t, err)
	putRes, err := kvClient.Put(&xlineapi.PutRequest{
		Key:   []byte("compact"),
		Value: []byte("1"),
	})

	assert.NoError(t, err)
	rev := putRes.Header.Revision

	// before compacting
	rev0res, err := kvClient.Range(&xlineapi.RangeRequest{
		Key:      []byte("compact"),
		Revision: rev - 1,
	})
	assert.NoError(t, err)
	assert.NotNil(t, rev0res)
	assert.Equal(t, "0", string(rev0res.Kvs[0].Value))

	rev1res, err := kvClient.Range(&xlineapi.RangeRequest{
		Key:      []byte("compact"),
		Revision: rev,
	})
	assert.NoError(t, err)
	assert.NotNil(t, rev1res)
	assert.Equal(t, "1", string(rev1res.Kvs[0].Value))

	_, err = kvClient.Compact(&xlineapi.CompactionRequest{
		Revision: rev,
	})
	assert.NoError(t, err)

	// after compacting
	_, err = kvClient.Range(&xlineapi.RangeRequest{
		Key:      []byte("compact"),
		Revision: rev - 1,
	})
	assert.Error(t, err)

	rangeRes, err := kvClient.Range(&xlineapi.RangeRequest{
		Key:      []byte("compact"),
		Revision: rev,
	})
	assert.NoError(t, err)
	assert.NotNil(t, rangeRes)
	assert.Equal(t, "1", string(rangeRes.Kvs[0].Value))
}
