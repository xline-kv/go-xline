package client

import (
	"errors"
	"fmt"

	xlineapi "github.com/xline-kv/go-xline/api/xline"
)

type CommandError struct {
	err *xlineapi.ExecuteError
}

func (e *CommandError) Error() string {
	return fmt.Sprintf("command error: %v", e.err)
}

var ErrTimeout = errors.New("timeout")
var ErrWrongClusterVersion = errors.New("wrong cluster version")
var ErrShuttingDown = errors.New("shutting down")
var ErrRpcTransport = errors.New("rpc transport")
var ErrRedirect = errors.New("redirect")