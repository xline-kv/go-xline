package client

// Client for Election operations.
type electionClient struct{}

// Create a new election client
func newElectionClient() electionClient {
	return electionClient{}
}
