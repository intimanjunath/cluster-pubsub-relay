package proxy

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"

	"github.com/gorilla/websocket"

	"flux_balancer/internal/discovery"
	"flux_balancer/internal/hash"
)

type SubscribeRequest struct {
	SubscriberID string `json:"subscriber_id"`
	Topic        string `json:"topic"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true }, // loosen for dev
}

func ProxySubscribe(w http.ResponseWriter, r *http.Request) {
	// Step 1: Upgrade client HTTP connection to WebSocket
	clientConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[ProxySubscribe] Failed to upgrade WebSocket: %v", err)
		http.Error(w, "WebSocket upgrade failed", http.StatusBadRequest)
		return
	}
	defer clientConn.Close()

	// Step 2: Read initial subscribe request from client
	_, msg, err := clientConn.ReadMessage()
	if err != nil {
		log.Printf("[ProxySubscribe] Failed to read subscription request: %v", err)
		return
	}

	var req SubscribeRequest
	if err := json.Unmarshal(msg, &req); err != nil || req.SubscriberID == "" || req.Topic == "" {
		log.Printf("[ProxySubscribe] Invalid subscription payload: %s", string(msg))
		clientConn.WriteMessage(websocket.TextMessage, []byte(`{"error":"Invalid subscription request"}`))
		return
	}
	log.Printf("[ProxySubscribe] Subscriber=%s, Topic=%s", req.SubscriberID, req.Topic)

	// Step 3: Choose FluxNode pod
	podIPs := discovery.GetFluxNodeAddrs()
	if len(podIPs) == 0 {
		clientConn.WriteMessage(websocket.TextMessage, []byte(`{"error":"No fluxnode pods available"}`))
		return
	}
	index := hash.GetConsistentIndex(req.Topic, podIPs)
	if index == -1 {
		clientConn.WriteMessage(websocket.TextMessage, []byte(`{"error":"Hashing failed"}`))
		return
	}
	target := fmt.Sprintf("ws://%s:%s/subscribe", podIPs[index], DEFAULT_FLUXNODE_PORT)
	log.Printf("[ProxySubscribe] Routing to fluxnode pod: %s", target)

	// Step 4: Connect to the chosen fluxnode pod
	fluxNodeURL := url.URL{Scheme: "ws", Host: podIPs[index] + ":" + DEFAULT_FLUXNODE_PORT, Path: "/subscribe"}
	fluxNodeConn, _, err := websocket.DefaultDialer.Dial(fluxNodeURL.String(), nil)
	if err != nil {
		log.Printf("[ProxySubscribe] Failed to connect to fluxnode pod: %v", err)
		clientConn.WriteMessage(websocket.TextMessage, []byte(`{"error":"Failed to connect to fluxnode pod"}`))
		return
	}
	defer fluxNodeConn.Close()

	// Step 5: Forward initial message to fluxnode
	if err := fluxNodeConn.WriteMessage(websocket.TextMessage, msg); err != nil {
		log.Printf("[ProxySubscribe] Failed to forward subscription request: %v", err)
		return
	}

	// Step 6: Bidirectional proxying
	errc := make(chan error, 2)

	go proxyWebSocket(clientConn, fluxNodeConn, errc)
	go proxyWebSocket(fluxNodeConn, clientConn, errc)

	<-errc // wait for either direction to fail
}

func proxyWebSocket(src, dst *websocket.Conn, errc chan error) {
	for {
		msgType, msg, err := src.ReadMessage()
		if err != nil {
			errc <- err
			return
		}
		if err := dst.WriteMessage(msgType, msg); err != nil {
			errc <- err
			return
		}
	}
}
