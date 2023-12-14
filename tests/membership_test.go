package test

import (
	"fmt"
	"testing"

	"github.com/xline-kv/go-xline/client"
	"github.com/xline-kv/go-xline/xlog"
	"go.uber.org/zap/zapcore"
)

func TestMembership(t *testing.T)  {
	xlog.SetLevel(zapcore.InfoLevel)

	cli, _ := client.Connect([]string{"127.0.0.1:5555"})
	kv := cli.KV

	res, _ := kv.Put([]byte("hello"), []byte("xline"))
	fmt.Println(res)
}