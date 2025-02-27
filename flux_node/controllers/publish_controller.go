package controllers

import (
	"encoding/json"
	"io"
	"net/http"
	"log"

	"github.com/gorilla/websocket"
)

type PublishRequest struct {
	Topic   string `json:"topic"`
	Message string `json:"message"`
}

func PublishHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req PublishRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Invalid JSON format", http.StatusBadRequest)
		return
	}

	if req.Topic == "" || req.Message == "" {
		http.Error(w, "Both 'topic' and 'message' are required", http.StatusBadRequest)
		return
	}

	InitTopic(req.Topic)

	msg := []byte(req.Message)
	AddMessage(req.Topic, msg)

	subscriberMutex.RLock()
	subscribers, ok := Subscribers[req.Topic]
	subscriberMutex.RUnlock()

	if ok {
		for s := range subscribers {
			go func(sub *Subscriber) {
				sub.WriteLock.Lock()
				err := sub.Conn.WriteMessage(websocket.BinaryMessage, msg)
				sub.WriteLock.Unlock()

				if err != nil {
					log.Printf("[Fanout] Failed to write to subscriber: %v", err)
					removeSubscriber(req.Topic, sub)
				}
			}(s)
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"message accepted"}`))
}
