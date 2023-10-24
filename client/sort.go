package client

type SortOption struct {
	Target SortTarget
	Order  SortOrder
}

const (
	SortNone SortOrder = iota
	SortAscend
	SortDescend
)

const (
	SortByKey SortTarget = iota
	SortByVersion
	SortByCreateRevision
	SortByModRevision
	SortByValue
)

type SortTarget int
type SortOrder int
