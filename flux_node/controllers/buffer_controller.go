package controllers

import (
	"sync"
	"time"
	"log"
	"os"
	"fmt"

	"flux_node/services"
)

type DualBuffer struct {
	Buffer1       [][]byte
	Buffer2       [][]byte
	Size1         int64
	Size2         int64
	CurrentActive int // 0 = Buffer1 is active, 1 = Buffer2 is active
	Mutex         sync.RWMutex

	CurrentFile			*os.File
	CurrentFileSize		int64
	CurrentFileCreated	time.Time
}

var TopicBuffers = make(map[string]*DualBuffer)
var topicBufferMutex sync.RWMutex

const BUFFER_SIZE = 10 * 1024 * 1024 // 10 MB
const BUFFER_ROTATION_INTERVAL = 10 // 10 Seconds
const FLUSH_BATCH_SIZE = 10*BUFFER_SIZE // 100 MB
const FLUSH_BATCH_INTERVAL = 10*time.Minute // 10 Minutes
const FLUSH_CHECK_INTERVAL = 1*time.Second // 1 Seconds

func InitTopic(topic string) {
	topicBufferMutex.Lock()
	defer topicBufferMutex.Unlock()

	if _, exists := TopicBuffers[topic]; !exists {

		filename := fmt.Sprintf("%s_%d.dat", topic, time.Now().Unix())
		new_file,err := os.Create(filename)
		if err != nil{
			log.Fatalf("Failed to create .dat file: %v", err)
		}

		TopicBuffers[topic] = &DualBuffer{
			Buffer1:       [][]byte{},
			Buffer2:       [][]byte{},
			Size1:         0,
			Size2:         0,
			CurrentActive: 0,
			CurrentFile: new_file,
			CurrentFileSize: 0,
			CurrentFileCreated: time.Now(),
		}
		go startBufferRotation(TopicBuffers[topic], BUFFER_ROTATION_INTERVAL*time.Second)
		go startDataFileFlushLoop(topic, TopicBuffers[topic])
	}
}

func AddMessage(topic string, message []byte) {
	topicBufferMutex.RLock()
	defer topicBufferMutex.RUnlock()

	if buffer, ok := TopicBuffers[topic]; ok {
		buffer.Mutex.Lock()
		defer buffer.Mutex.Unlock()

		if buffer.CurrentActive == 0 {
			buffer.Buffer1 = append(buffer.Buffer1, message)
			buffer.Size1 += int64(len(message))
		} else {
			buffer.Buffer2 = append(buffer.Buffer2, message)
			buffer.Size2 += int64(len(message))
		}

		n,err := buffer.CurrentFile.Write(append(message, '\n'))
		if err != nil{
			log.Printf("[Topic: %s] Error writing to .dat file: %v", topic, err)
		}else{
			buffer.CurrentFileSize += int64(n)
		}

		if buffer.CurrentActive == 0 && buffer.Size1 >= BUFFER_SIZE {
			buffer.Buffer2 = nil
			buffer.Size2 = 0
			buffer.CurrentActive = 1
		}
		if buffer.CurrentActive == 1 && buffer.Size2 >= BUFFER_SIZE {
			buffer.Buffer1 = nil
			buffer.Size1 = 0
			buffer.CurrentActive = 0
		}

	}
}

func GetCurrentBuffer(topic string) [][]byte {
	topicBufferMutex.RLock()
	defer topicBufferMutex.RUnlock()

	if buffer, ok := TopicBuffers[topic]; ok {
		buffer.Mutex.RLock()
		defer buffer.Mutex.RUnlock()

		if buffer.CurrentActive == 0 {
			return buffer.Buffer1
		}
		return buffer.Buffer2
	}
	return nil
}

func startBufferRotation(buffer *DualBuffer, interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		buffer.Mutex.Lock()
		if buffer.CurrentActive == 0 {
			buffer.Buffer2 = nil
			buffer.Size2 = 0
			buffer.CurrentActive = 1
		} else {
			buffer.Buffer1 = nil
			buffer.Size1 = 0
			buffer.CurrentActive = 0
		}
		buffer.Mutex.Unlock()
	}
}

func uploadAndRotateDataFile(topic string, buffer *DualBuffer){
	buffer.Mutex.Lock()
	defer buffer.Mutex.Unlock()

	oldFile := buffer.CurrentFile
	oldFileName := oldFile.Name()
	oldFile.Close()

	log.Printf("[Topic: %s] Uploading file to S3: %s", topic, oldFileName)
	err := services.UploadFileToS3(topic, oldFileName)
	if err != nil {
		log.Printf("[Topic: %s] S3 upload failed: %v", topic, err)
	}

	newFileName := fmt.Sprintf("%s_%d.dat", topic, time.Now().Unix())
	newFile, err := os.Create(newFileName)
	if err != nil {
		log.Printf("[Topic: %s] Failed to create new .dat file: %v", topic, err)
		return
	}

	buffer.CurrentFile = newFile
	buffer.CurrentFileSize = 0
	buffer.CurrentFileCreated = time.Now()

	log.Printf("[Topic: %s] Switched to new file: %s", topic, newFileName)
}

func startDataFileFlushLoop(topic string, buffer *DualBuffer){
	ticker := time.NewTicker(FLUSH_CHECK_INTERVAL)	// check every n seconds
	for range ticker.C {
		buffer.Mutex.Lock()

		now := time.Now()
		elapsed := now.Sub(buffer.CurrentFileCreated)

		// If file size exceeded, flush immediately
		if buffer.CurrentFileSize >= FLUSH_BATCH_SIZE {
			log.Printf("[Topic: %s] Flushing .dat file due to size (%d bytes)", topic, buffer.CurrentFileSize)
			go uploadAndRotateDataFile(topic, buffer)
			buffer.Mutex.Unlock()
			continue
		}

		// If time-based interval passed, flush only if there's data
		if elapsed >= FLUSH_BATCH_INTERVAL {
			if buffer.CurrentFileSize > 0 {
				log.Printf("[Topic: %s] Flushing .dat file due to time (%v)", topic, elapsed)
				go uploadAndRotateDataFile(topic, buffer)
			}
			// else {
			// 	log.Printf("[Topic: %s] Skipping flush. No messages in the last %v", topic, FLUSH_BATCH_INTERVAL)
			// }
		}

		buffer.Mutex.Unlock()
	}
}
