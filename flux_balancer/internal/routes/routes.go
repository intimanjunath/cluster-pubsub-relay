package routes

import (
	"net/http"

	"github.com/gorilla/mux"
	"flux_balancer/internal/proxy"
)

func SetupRoutes() http.Handler {
	r := mux.NewRouter()
	r.HandleFunc("/publish", proxy.ProxyPublish).Methods("POST")
	r.HandleFunc("/subscribe", proxy.ProxySubscribe).Methods("GET")
	return r
}
