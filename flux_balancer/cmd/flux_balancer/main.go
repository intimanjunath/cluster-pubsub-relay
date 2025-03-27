package main

import (
	// "fmt"
	"log"
	"os"
	"net/http"

	"github.com/joho/godotenv"

	"flux_balancer/internal/discovery"
	"flux_balancer/internal/routes"
)

func main() {
	
	env := os.Getenv("ENV")
	if env == "dev_local" {
		err := godotenv.Load()
		if err != nil {
			log.Println("No .env file found, using system environment")
		} else {
			log.Println(".env file loaded successfully")
		}
	} else {
		log.Println("Running in", env, "— using environment variables only")
	}

	discovery.StartWatcher()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8083"
	}

	log.Printf("[flux-balancer] Listening on :%s", port)
	err := http.ListenAndServe(":"+port, routes.SetupRoutes())
	if err != nil {
		log.Fatalf("server error: %v", err)
	}

	// Start your routes + server here later
	http.ListenAndServe(":8080", nil)
}
