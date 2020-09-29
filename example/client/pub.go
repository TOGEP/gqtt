package main

import (
	"context"
	"github.com/ysugimoto/gqtt"
	"github.com/ysugimoto/gqtt/message"
	"log"
	"strconv"
	"sync"
	"time"
)

func main() {
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(3)
		go connect(&wg, 1)
		go connect(&wg, 10)
		go connect(&wg, 20)
	}
	wg.Wait()
}

func connect(wg *sync.WaitGroup, count int) {
	defer wg.Done()
	var sig string = strconv.Itoa(count)

	client := gqtt.NewClient("mqtt://localhost:1883")
	defer client.Disconnect()

	ctx := context.Background()
	auth := gqtt.WithLoginAuth("admin", "admin")
	will := gqtt.WithWill(message.QoS0, false, "mqtt/test", "send will", nil)
	if err := client.Connect(ctx, auth, will); err != nil {
		log.Fatal(err)
	}
	log.Println("client connected")

	ticker := time.NewTicker(3 * time.Second)

	for {
		select {
		case <-client.Closed:
			log.Println("connection closed")
			return
		case <-ctx.Done():
			log.Println("context canceled")
			return
		case <-ticker.C:
			log.Printf("message publish")
			ticker.Stop()
			i, _ := strconv.Atoi(sig)
			switch {
			case i >= 0 && i <= 9:
				if err := client.Publish("mqtt/test", []byte("urgent packet from device"+sig), gqtt.WithQoS(message.QoS0), gqtt.WithUserProperty(map[string]string{"priority": "urgent"})); err != nil {
					return
				}
			case i >= 10 && i <= 19:
				if err := client.Publish("mqtt/test", []byte("critical packet from device"+sig), gqtt.WithQoS(message.QoS1), gqtt.WithUserProperty(map[string]string{"priority": "critical"})); err != nil {
					return
				}
			case i >= 20 && i <= 29:
				if err := client.Publish("mqtt/test", []byte("normal packet from device"+sig), gqtt.WithQoS(message.QoS2), gqtt.WithUserProperty(map[string]string{"priority": "normal"})); err != nil {
					return
				}
			default:
				if err := client.Publish("mqtt/test", []byte("random packet from device"+sig), gqtt.WithQoS(message.QoS0), gqtt.WithUserProperty(map[string]string{"priority": "random"})); err != nil {
					return
				}
			}
		}
	}
}
