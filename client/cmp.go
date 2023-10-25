package client

import xlineapi "github.com/xline-kv/go-xline/api/xline"

type cmp xlineapi.Compare

func compare(cmp *cmp, result string) cmp {
	var r xlineapi.Compare_CompareResult

	switch result {
	case "=":
		r = xlineapi.Compare_EQUAL
	case "!=":
		r = xlineapi.Compare_NOT_EQUAL
	case ">":
		r = xlineapi.Compare_GREATER
	case "<":
		r = xlineapi.Compare_LESS
	default:
		panic("Unknown result op")
	}

	cmp.Result = r
}
