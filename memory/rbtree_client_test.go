package memory

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func compareTwoRBTreeClient(src *RBTreeClient, dst *RBTreeClient) bool {
	clientAreSame := true
	if src.cacheData.Size() != dst.cacheData.Size() {
		clientAreSame = false
	}
	return clientAreSame
}

func TestRBTreeClientSet(t *testing.T) {
	testCases := []struct {
		name        string
		startClient func() *RBTreeClient
		key         string
		value       string
		expiration  time.Duration
		wantClient  func() *RBTreeClient
		wantErr     error
	}{
		{
			name: "set value to empty cache",
			startClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(ComparatorRBTreeUnit())
				return client
			},
			key:        "key1",
			value:      "value1",
			expiration: time.Minute,
			wantClient: func() *RBTreeClient {
				client, _ := NewRBTreeClient(ComparatorRBTreeUnit())
				_ = client.cacheData.Add("key1", &rbTreeNode{})
				return client
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startClient := tc.startClient()
			err := startClient.Set(context.Background(), tc.key, tc.value, tc.expiration)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			clientAreSame := compareTwoRBTreeClient(startClient, tc.wantClient())
			assert.Equal(t, true, clientAreSame)
		})
	}
}
