package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"log"

	"flux_balancer/internal/discovery"
	"flux_balancer/internal/hash"
)

type PublishRequest struct {
	PublisherID string `json:"publisher_id"`
	Topic       string `json:"topic"`
	Message     string `json:"message"`
}

func ProxyPublish(w http.ResponseWriter, r *http.Request) {
	var req PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	log.Printf("[ProxyPublish] Received message: PublisherID=%s Topic=%s", req.PublisherID, req.Topic)

	podIPs := discovery.GetFluxNodeAddrs()
	if len(podIPs) == 0 {
		http.Error(w, "no emitter pods available", http.StatusServiceUnavailable)
		return
	}

	index := hash.GetConsistentIndex(req.Topic, podIPs)
	if index == -1 {
		http.Error(w, "hashing failed", http.StatusInternalServerError)
		return
	}

	target := fmt.Sprintf("http://%s:%s/publish", podIPs[index], DEFAULT_FLUXNODE_PORT)

	log.Printf("[ProxyPublish] Routing to emitter pod: %s", target)

	bodyBytes, _ := json.Marshal(req)
	proxyReq, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(bodyBytes))
	if err != nil {
		http.Error(w, "failed to create proxy request", http.StatusInternalServerError)
		return
	}
	proxyReq.Header = r.Header

	resp, err := http.DefaultClient.Do(proxyReq)
	if err != nil {
		http.Error(w, "failed to reach emitter pod", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	log.Printf("[ProxyPublish] Forwarded successfully. StatusCode=%d", resp.StatusCode)

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}
