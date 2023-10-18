package client

// Client for Cluster operations.
type clusterClient struct{}

// Create a new cluster client
func newClusterClient() clusterClient {
	return clusterClient{}
}
