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

var ErrWrongClusterVersion = errors.New("wrong cluster version")
