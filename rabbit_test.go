package driver

import (
	"context"
	"github.com/rabbitmq/amqp091-go"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var conn *amqp091.Connection

func init() {
	conn, _ = amqp091.Dial("amqp://rabbitmq:123456666@localhost:5672/")
}

func TestRabbitDriver(t *testing.T) {
	var num uint32
	var driver = NewRabbitMQ(conn, 2)
	var errChan = make(chan error, 2)
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

	assert.Equal(t, uint32(2), num)
}
