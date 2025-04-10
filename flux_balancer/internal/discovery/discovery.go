package discovery

import (
	"context"
	"log"
	"os"
	"time"
	"sync"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Discovery struct {
	clientset *kubernetes.Clientset
	namespace string
}

var (
	mutex        sync.RWMutex
	fluxNodeAddrs []string
)

func StartWatcher() {
	go watchPods("app=flux-node", &fluxNodeAddrs)
}

func watchPods(labelSelector string, store *[]string) {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Failed to load in-cluster config: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create clientset: %v", err)
	}

	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}

	for {
		pods, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
			LabelSelector: labelSelector,
		})
		if err != nil {
			log.Printf("Failed to list pods for %s: %v", labelSelector, err)
			time.Sleep(5 * time.Second)
			continue
		}

		var updated []string
		for _, pod := range pods.Items {

			if pod.Status.Phase == v1.PodRunning {
				ip := pod.Status.PodIP

				if ip != "" {
					updated = append(updated, ip)
				}
			}
		}

		mutex.Lock()
		*store = updated
		mutex.Unlock()

		time.Sleep(5 * time.Second)
	}
}

func GetFluxNodeAddrs() []string {
	mutex.RLock()
	defer mutex.RUnlock()
	return append([]string(nil), fluxNodeAddrs...) // return a copy
}
