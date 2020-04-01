package driver

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestRabbitDriver(t *testing.T) {
	var conn, err = amqp.Dial("amqp://localhost:5672")

	assert.Nil(t, err)

	var num uint32
	var driver = New(conn)
	var errChan = make(chan error, 1)
	var msgData = []byte("test.message")

	assert.Nil(t, driver.CreateTopic("test.topic"))
	assert.Nil(t, driver.CreateQueue("test.queue", time.Second))
	assert.Nil(t, driver.Subscribe("test.topic", "test.queue", "test"))
	assert.Nil(t, driver.SendToTopic("test.topic", msgData, "test"))

	ctx, cancelFunc := context.WithCancel(context.TODO())

	driver.ReceiveMessage(ctx, "test.queue", errChan, func(bytes []byte) bool {
		assert.Equal(t, msgData, bytes)
		if atomic.AddUint32(&num, 1) == 1 {
			assert.Nil(t, driver.SendToQueue("test.queue", bytes, time.Second*2))
		} else {
			cancelFunc()
		}
		return true
	})

	<-errChan
	assert.Equal(t, uint32(2), num)
}
