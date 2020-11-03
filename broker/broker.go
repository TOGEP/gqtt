package broker

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/TOGEP/gqtt/priority_channel"
	"github.com/pkg/errors"
	"github.com/ysugimoto/gqtt/internal/log"
	"github.com/ysugimoto/gqtt/message"
)

const (
	capEventSize = 100
)

func formatTopicPath(path string) string {
	return "/" + strings.Trim(path, "/")
}

var clients = make(map[string]*net.Conn)

type Broker struct {
	addr          string
	subscription  *Subscription
	willPacketId  uint16
	MessageEvent  chan interface{}
	PriorityQueue *priority_channel.PriorityChannel

	mu sync.Mutex
}

func NewBroker(addr string) *Broker {
	return &Broker{
		addr:          addr,
		subscription:  NewSubscription(),
		MessageEvent:  make(chan interface{}, capEventSize),
		PriorityQueue: priority_channel.NewPriorityChannel(),
	}
}

func (b *Broker) ListenAndServe(ctx context.Context) error {
	listener, err := net.Listen("tcp", b.addr)
	if err != nil {
		return errors.Wrap(err, "failed to listen TCP socket")
	}

	defer listener.Close()
	log.Debugf("Broker server started at %s", b.addr)

	go func() {
		for {
			select {
			case pb := <-b.PriorityQueue.Out:
				if pb == nil {
					log.Debug("nothing queue data")
					return
				}
				//time.Sleep(time.Second) //DEBUG
				switch e := pb.(type) {
				case *message.PriorityPublishMessage:
					//TODO support priority publish QoS1&2
					log.Debugf("send priority message: %s\n", e.Message)
					if err := message.WriteFrame(*e.Conn, e.Message); err != nil {
						log.Debug("failed to send publish message: ", e)
					}
				default:
					log.Debug("type error")
				}
			}
		}
	}()

	for {
		s, err := listener.Accept()
		if err != nil {
			log.Debug(err)
			continue
		}

		info, err := b.handshake(s, 10*time.Second)
		if err != nil {
			log.Debug("Failed to MQTT handshake: ", err.Error())
			s.Close()
			continue
		}
		client := NewClient(s, *info, ctx, b)
		go b.handleConnection(client)
	}
}

func (b *Broker) sendEvent(msg interface{}) {
	// Check overflow channel buffer
	if len(b.MessageEvent) >= capEventSize {
		log.Debug("Event channle overflow. You have to drain message")
	} else {
		b.MessageEvent <- msg
	}
}

func (b *Broker) handshake(conn net.Conn, timeout time.Duration) (*message.Connect, error) {
	conn.SetDeadline(time.Now().Add(timeout))
	var (
		err     error
		reason  message.ReasonCode
		frame   *message.Frame
		payload []byte
		cn      *message.Connect
	)
	defer func() {
		log.Debug("defer: send CONNACK")
		ack := message.NewConnAck(reason)
		if err != nil {
			ack.Property = &message.ConnAckProperty{
				ReasonString: err.Error(),
			}
		}
		if err := message.WriteFrame(conn, ack); err != nil {
			log.Debug("failed to send CONNACK: ", err)
		}
		conn.SetDeadline(time.Time{})
	}()

	frame, payload, err = message.ReceiveFrame(conn)
	if err != nil {
		log.Debug("receive frame error: ", err)
		reason = message.MalformedPacket
		return nil, errors.Wrap(err, "failed to receive packet")
	}
	cn, err = message.ParseConnect(frame, payload)
	if err != nil {
		reason = message.MalformedPacket
		log.Debug("frame expects connect package: ", err)
		return nil, errors.Wrap(err, "Malformed packet received")
	}
	if err := b.authConnect(conn, cn.Property); err != nil {
		reason = message.NotAuthorized
		log.Debug("connection not authorized")
		return nil, errors.Wrap(err, "Not Authorized")
	}
	reason = message.Success
	log.Debugf("CONNECT accepted")
	b.sendEvent(cn)
	return cn, nil
}

func (b *Broker) authConnect(conn net.Conn, cp *message.ConnectProperty) error {
	// TODO: control to need to authneication on broker from setting or someway
	if cp == nil {
		return nil
	}
	switch cp.AuthenticationMethod {
	case basicAuthentication:
		return doBasicAuth(conn, cp)
	case loginAuthentication:
		return doLoginAuth(conn, cp)
	default:
		return fmt.Errorf("%s does not support or unrecognized", cp.AuthenticationMethod)
	}
}

func (b *Broker) handleConnection(client *Client) {
	b.addClient(client)

	defer func() {
		log.Debug("====== Client closing ======")
		b.removeClient(client.Id())
		client.Close(true)
	}()

	for {
		select {
		case <-client.Closed():
			log.Debug("client context has been canceled")
			return
		}
	}
}

func (b *Broker) addClient(client *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	clients[client.Id()] = client.Conn()
}

func (b *Broker) removeClient(clientId string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := clients[clientId]; ok {
		delete(clients, clientId)
		b.subscription.UnsubscribeAll(clientId)
	}
}

func (b *Broker) Publish(pb *message.Publish) {
	si := b.subscription.GetClientsByTopic(pb.TopicName)
	if si == nil {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	log.Debug("start to send publish packet")
	b.sendEvent(pb)

	// Save as retain message is RETAIN bit is active
	if pb.RETAIN {
		// TODO: persistent save to DB, or some backend
		log.Debug("Save retian message to topic: ", pb.TopicName)
		si.RetainMessage = pb
	}
	// And we always set retain as set in order to distinguish retained message or not in client.
	pb.SetRetain(false)

	for cid, qos := range si.Clients {
		c, ok := clients[cid]
		if !ok {
			continue
		}

		// Downgrade QoS if we need
		if pb.QoS > qos {
			log.Debugf("send publish message to: %s (downgraded %d -> %d)\n", cid, pb.QoS, qos)
			downpb := pb.Downgrade(qos)
			priority := pb.Property.UserProperty["priority"]
			message.AddPriorityPublishMessage(c, downpb, b.PriorityQueue, priority)
		} else {
			log.Debugf("send publish message to: %s with qos: %d", cid, pb.QoS)
			priority := pb.Property.UserProperty["priority"]
			message.AddPriorityPublishMessage(c, pb, b.PriorityQueue, priority)
		}
	}
}

func (b *Broker) subscribe(client *Client, ss *message.Subscribe) (message.Encoder, error) {
	rcs := []message.ReasonCode{}
	// TODO: confirm subscription settings e.g. max QoS, ...
	for _, t := range ss.Subscriptions {
		rc, err := b.subscription.Subscribe(client.Id(), t)
		if err != nil {
			return nil, errors.Wrap(err, "failed to subscribe: "+t.TopicName)
		}
		rcs = append(rcs, rc)
	}
	b.sendEvent(ss)
	return message.NewSubAck(ss.PacketId, rcs...), nil
}

func (b *Broker) will(c message.Connect) {
	if !c.FlagWill {
		log.Debug("client didn't want to use will. Skip")
		return
	}
	log.Debugf("client wants to send will message: qos: %d, topic: %s, body: %s", c.WillQoS, c.WillTopic, c.WillPayload)
	b.willPacketId++
	pb := message.NewPublish(b.willPacketId, message.WithQoS(c.WillQoS))
	pb.SetRetain(c.WillRetain)
	pb.TopicName = c.WillTopic
	pb.Body = []byte(c.WillPayload)
	if c.WillProperty != nil {
		pb.Property = c.WillProperty.ToPublish()
	}
	b.Publish(pb)
}

func (b *Broker) getRetainMessage(topicName string) *message.Publish {
	if si := b.subscription.GetClientsByTopic(topicName); si != nil {
		return si.RetainMessage
	}
	return nil
}

func (b *Broker) deleteRetainMessage(topicName string) {
	if si := b.subscription.GetClientsByTopic(topicName); si != nil {
		// TODO: delete from persistent storage
		si.RetainMessage = nil
	}
}
