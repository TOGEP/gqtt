package main

import (
	"context"
	"github.com/TOGEP/gqtt"
	"github.com/ysugimoto/gqtt/message"
	"log"
	"os"
	"time"
)

func main() {
	var sig string = "default"
	if len(os.Args) > 1 {
		sig = os.Args[1]
	}

	client := gqtt.NewClient("mqtt://localhost:1883")
	defer client.Disconnect()

	ctx := context.Background()
	auth := gqtt.WithLoginAuth("admin", "admin")
	will := gqtt.WithWill(message.QoS0, false, "mqtt/test", "send will", nil)
	if err := client.Connect(ctx, auth, will); err != nil {
		log.Fatal(err)
	}
	log.Println("client connected")

	if err := client.Subscribe("mqtt/test", message.QoS2); err != nil {
		log.Fatal(err)
	}
	log.Println("subscribed")

	ticker := time.NewTicker(3 * time.Second)

	for {
		select {
		case <-client.Closed:
			log.Println("connection closed")
			return
		case <-ctx.Done():
			log.Println("context canceled")
			return
		case msg := <-client.Message:
			log.Printf("published message received: %s\n", string(msg.Body))
		case <-ticker.C:
			log.Printf("message publish")
			ticker.Stop()
			if err := client.Publish("mqtt/test", []byte("Hello, MQTT5 UserProperty! from "+sig), gqtt.WithQoS(message.QoS2), gqtt.WithUserProperty(map[string]string{"userproperty-key": "userproperty-value"})); err != nil {
				return
			}
		}
	}
}
