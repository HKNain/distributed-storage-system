package middleware

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

const leaderForwardTimeout = 30 * time.Minute

// ForwardToLeader proxies requests to the current Raft leader when this
// master is a follower. The request body is streamed without buffering it.
func ForwardToLeader(
	raftNode *raft.Raft,
	httpPeers map[string]string,
	next http.Handler,
) http.Handler {
	client := &http.Client{Timeout: leaderForwardTimeout}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if raftNode == nil || raftNode.State() == raft.Leader {
			next.ServeHTTP(w, r)
			return
		}

		leaderRaftAddress := string(raftNode.Leader())
		leaderHTTPAddress, ok := httpPeers[leaderRaftAddress]
		if !ok || strings.TrimSpace(leaderHTTPAddress) == "" {
			http.Error(
				w,
				"Leader is not available for upload forwarding",
				http.StatusServiceUnavailable,
			)
			return
		}

		forwardURL := "http://" + leaderHTTPAddress + r.URL.RequestURI()
		forwardRequest, err := http.NewRequestWithContext(
			r.Context(),
			r.Method,
			forwardURL,
			r.Body,
		)
		if err != nil {
			http.Error(w, "Could not forward request", http.StatusBadGateway)
			return
		}

		forwardRequest.ContentLength = r.ContentLength
		for key, values := range r.Header {
			for _, value := range values {
				forwardRequest.Header.Add(key, value)
			}
		}

		response, err := client.Do(forwardRequest)
		if err != nil {
			http.Error(
				w,
				fmt.Sprintf("Could not reach leader: %v", err),
				http.StatusBadGateway,
			)
			return
		}
		defer response.Body.Close()

		for key, values := range response.Header {
			w.Header()[key] = append([]string(nil), values...)
		}

		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
	})
}
