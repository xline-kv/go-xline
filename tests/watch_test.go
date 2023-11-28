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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xline-kv/go-xline/client"
	"github.com/xline-kv/go-xline/xlog"
	"go.uber.org/zap/zapcore"
)

func TestWatch(t *testing.T) {
	xlog.SetLevel(zapcore.ErrorLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	client, err := client.Connect(curpMembers)
	assert.NoError(t, err)
	kvClient := client.KV
	watchClient := client.Watch

	t.Run("watch_should_receive_consistent_events", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		ch, _ := watchClient.Watch(ctx, []byte("watch01"))

		go func() {
			kvClient.Put([]byte("watch01"), []byte("01"))
			kvClient.Put([]byte("watch01"), []byte("02"))
			kvClient.Put([]byte("watch01"), []byte("03"))
			cancel()
		}()

		var watchID int64
		i := 1
		for res := range ch {
			switch i {
			case 1:
				watchID = res.WatchId
				assert.True(t, res.Created)
			case 2:
				assert.Equal(t, watchID, res.WatchId)
				assert.Equal(t, "watch01", string(res.Events[0].Kv.Key))
				assert.Equal(t, "01", string(res.Events[0].Kv.Value))
			case 3:
				assert.Equal(t, watchID, res.WatchId)
				assert.Equal(t, "watch01", string(res.Events[0].Kv.Key))
				assert.Equal(t, "02", string(res.Events[0].Kv.Value))
			case 4:
				assert.Equal(t, watchID, res.WatchId)
				assert.Equal(t, "watch01", string(res.Events[0].Kv.Key))
				assert.Equal(t, "03", string(res.Events[0].Kv.Value))
			case 5:
				assert.Equal(t, watchID, res.WatchId)
				assert.True(t, res.Canceled)
			}
			i++
		}
	})
}
