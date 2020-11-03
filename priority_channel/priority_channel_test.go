package priority_channel_test

import (
	"fmt"
	"github.com/ysugimoto/gqtt/broker"
	"testing"
)

func TestPriorityChannel(t *testing.T) {
	pc := broker.NewPriorityChannel()
	pc.Normal <- 1
	pc.Critical <- 2
	pc.Urgent <- 3

	a := <-pc.Out

	if a != 3 {
		t.Fatal("hogeeeee")
	}
}
