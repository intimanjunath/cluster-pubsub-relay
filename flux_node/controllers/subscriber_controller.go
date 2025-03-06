package controllers

import (
	"log"
	"net/http"
	"sync"
	"encoding/json"

	"github.com/gorilla/websocket"
)

type Subscriber struct {
	Conn      *websocket.Conn
	WriteLock sync.Mutex
}

var Subscribers = make(map[string]map[*Subscriber]bool)
var subscriberMutex sync.RWMutex

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func SubscribeHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket upgrade failed:", err)
		return
	}

	_, msg, err := conn.ReadMessage()
	if err != nil {
		log.Println("[SubscribeHandler] Failed to read subscribe message:", err)
		conn.Close()
		return
	}

	var req struct {
		SubscriberID string `json:"subscriber_id"`
		Topic        string `json:"topic"`
	}
	if err := json.Unmarshal(msg, &req); err != nil || req.Topic == "" || req.SubscriberID == "" {
		log.Printf("[SubscribeHandler] Invalid subscribe payload: %s", string(msg))
		conn.WriteMessage(websocket.TextMessage, []byte(`{"error":"invalid subscribe request"}`))
		conn.Close()
		return
	}

	InitTopic(req.Topic)

	subscriber := &Subscriber{Conn: conn}

	subscriberMutex.Lock()
	if _, ok := Subscribers[req.Topic]; !ok {
		Subscribers[req.Topic] = make(map[*Subscriber]bool)
	}
	Subscribers[req.Topic][subscriber] = true
	subscriberMutex.Unlock()

	log.Printf("[Subscriber] Connected: %s on topic '%s'", req.SubscriberID, req.Topic)

	go waitForDisconnect(req.Topic, subscriber)
}

func waitForDisconnect(topic string, s *Subscriber) {
	for {
		_, _, err := s.Conn.NextReader()
		if err != nil {
			log.Printf("[Subscriber] Disconnected from topic '%s'", topic)
			removeSubscriber(topic, s)
			return
		}
	}
}

func removeSubscriber(topic string, s *Subscriber) {
	subscriberMutex.Lock()
	defer subscriberMutex.Unlock()

	if conns, ok := Subscribers[topic]; ok {
		delete(conns, s)
		s.Conn.Close()

		if len(conns) == 0 {
			delete(Subscribers, topic)
		}
	}
}
