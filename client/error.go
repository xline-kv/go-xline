package client

import (
	"fmt"

	"github.com/xline-kv/go-xline/api/xline"
)

type CommandError struct {
	err *xlineapi.ExecuteError
}

func (e *CommandError) Error() string {
	return fmt.Sprintf("command error: %v", e.err)
}
