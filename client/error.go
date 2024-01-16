package client

import (
	"errors"
	"fmt"

	"github.com/xline-kv/go-xline/api/gen/xline"
)

type CommandError struct {
	err *xlineapi.ExecuteError
}

func (e *CommandError) Error() string {
	return fmt.Sprintf("command error: %v", e.err)
}

type errInternalError struct {
	inner error
}

func NewErrInternalError(err error) error {
	return &errInternalError{
		inner: err,
	}
}

func (e *errInternalError) Error() string {
	return fmt.Sprintf("Client Internal error: %s", e.inner.Error())
}

var (
	ErrShuttingDown        = errors.New("Curp Server is shutting down")
	ErrWrongClusterVersion = errors.New("Wrong cluster version")
	ErrTimeout             = errors.New("Request timeout")
)
