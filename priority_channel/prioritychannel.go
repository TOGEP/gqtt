package priority_channel

import (
	"github.com/TOGEP/gqtt/internal/log"
)

type PriorityChannel struct {
	Out      chan interface{}
	Urgent   chan interface{}
	Critical chan interface{}
	Normal   chan interface{}
	stopCh   chan struct{}
}

func NewPriorityChannel() *PriorityChannel {
	pc := PriorityChannel{}
	pc.Out = make(chan interface{})
	pc.Urgent = make(chan interface{}, 1)
	pc.Critical = make(chan interface{}, 1)
	pc.Normal = make(chan interface{}, 1)
	pc.stopCh = make(chan struct{})

	pc.start()
	return &pc
}

func (pc *PriorityChannel) Close() {
	close(pc.stopCh)
	close(pc.Normal)
	close(pc.Critical)
	close(pc.Urgent)
	close(pc.Out)
}

func (pc *PriorityChannel) Enqueue(priority string, data interface{}) {
	switch priority {
	case "normal":
		log.Debug("add normal queue")
		pc.Normal <- data
	case "critical":
		log.Debug("add critical queue")
		pc.Critical <- data
	case "urgent":
		log.Debug("add urgent queue")
		pc.Urgent <- data
	default:
		log.Debug("add normal queue (another priority)")
		pc.Normal <- data
	}
}

func (pc *PriorityChannel) start() {
	go func() {
		for {
			select {
			case s := <-pc.Urgent:
				pc.Out <- s
				log.Debug("out<-urgent")
				continue
			case <-pc.stopCh:
				return
			default:
			}

			select {
			case s := <-pc.Urgent:
				pc.Out <- s
				log.Debug("out<-urgent")
				continue
			case s := <-pc.Critical:
				pc.Out <- s
				log.Debug("out<-critical")
				continue
			case <-pc.stopCh:
				return
			default:
			}

			select {
			case s := <-pc.Urgent:
				pc.Out <- s
				log.Debug("out<-urgent")
			case s := <-pc.Critical:
				pc.Out <- s
				log.Debug("out<-critical")
			case s := <-pc.Normal:
				pc.Out <- s
				log.Debug("out<-normal")
			case <-pc.stopCh:
				return
			}
		}
	}()
}
