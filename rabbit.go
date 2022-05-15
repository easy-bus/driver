package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/easy-bus/bus"
	"github.com/letsfire/factory"
	"github.com/rabbitmq/amqp091-go"
)

type rabbitDriver struct {
	conn *amqp091.Connection

	master *factory.Master

	// queues 延迟映射表
	queues map[string]map[time.Duration]string

	// initMap 首次申明映射
	initMap map[string]string
}

func (rd *rabbitDriver) maintainMap(name string, delay time.Duration, replace string) {
	if rd.queues[name] == nil {
		rd.queues[name] = make(map[time.Duration]string)
	}
	rd.queues[name][delay] = replace
	if _, ok := rd.initMap[name]; !ok {
		rd.initMap[name] = replace
	}
}

func (rd *rabbitDriver) CreateQueue(name string, delay time.Duration) error {
	return rd.callWithChannel(func(ch *amqp091.Channel) error {
		if _, err := ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
			return err
		}
		if delay == 0 {
			rd.maintainMap(name, delay, name)
			return nil
		}
		ms := delay.Milliseconds()
		rq := fmt.Sprintf("%s.delay-%d", name, ms)
		_, err := ch.QueueDeclare(rq, true, false, false, false, amqp091.Table{
			"x-message-ttl":             int(ms),
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": name,
		})
		if err == nil {
			rd.maintainMap(name, delay, rq)
		}
		return err
	})
}

func (rd *rabbitDriver) CreateTopic(name string) error {
	return rd.callWithChannel(func(ch *amqp091.Channel) error {
		return ch.ExchangeDeclare(name, "topic", true, false, false, false, nil)
	})
}

func (rd *rabbitDriver) Subscribe(topic, queue, routeKey string) error {
	return rd.callWithChannel(func(ch *amqp091.Channel) error {
		return ch.QueueBind(rd.initMap[queue], routeKey, topic, false, nil)
	})
}

func (rd *rabbitDriver) UnSubscribe(topic, queue, routeKey string) error {
	return rd.callWithChannel(func(ch *amqp091.Channel) error {
		return ch.QueueUnbind(rd.initMap[queue], routeKey, topic, nil)
	})
}

func (rd *rabbitDriver) SendToQueue(name string, content []byte, delay time.Duration) error {
	if _, ok := rd.queues[name][delay]; !ok {
		if err := rd.CreateQueue(name, delay); err != nil {
			return err
		}
	}
	return rd.callWithChannel(func(ch *amqp091.Channel) error {
		pb := amqp091.Publishing{Body: content}
		return ch.Publish("", rd.queues[name][delay], false, false, pb)
	})
}

func (rd *rabbitDriver) SendToTopic(name string, content []byte, routeKey string) error {
	return rd.callWithChannel(func(ch *amqp091.Channel) error {
		return ch.Publish(name, routeKey, false, false, amqp091.Publishing{Body: content})
	})
}

func (rd *rabbitDriver) ReceiveMessage(ctx context.Context, queue string, errChan chan error, handler func([]byte) bool) {
	if _, ok := rd.queues[queue]; !ok {
		panic(fmt.Sprintf("easy-bus-rabbit-driver: the queue %q does not exist, create it first", queue))
	}
	line := rd.master.AddLine(func(i interface{}) {
		var err error
		var msg = i.(amqp091.Delivery)
		if handler(msg.Body) {
			err = msg.Ack(false)
		} else {
			err = msg.Nack(false, true)
		}
		if err != nil {
			errChan <- err
		}
	})
	defer line.Wait()
	done, size := false, ctx.Value("prefetch_count")
	for done == false {
		select {
		case <-ctx.Done():
			return
		default:
			err := rd.callWithChannel(func(ch *amqp091.Channel) error {
				if size != nil {
					err := ch.Qos(size.(int), 0, false)
					if err != nil {
						return err
					}
				}
				msgChan, err := ch.Consume(queue, "", false, false, false, false, nil)
				if err != nil {
					return err
				}
				for {
					select {
					case <-ctx.Done():
						done = true
						return nil
					case msg := <-msgChan:
						// delivery not initialized
						if msg.Acknowledger == nil {
							return nil // renew channel
						}
						line.Submit(msg)
					}
				}
			})
			if err != nil {
				errChan <- err
				time.Sleep(time.Second)
			}
		}
	}
}

func (rd *rabbitDriver) callWithChannel(fn func(ch *amqp091.Channel) error) error {
	if ch, err := rd.conn.Channel(); err != nil {
		return err
	} else {
		defer ch.Close()
		return fn(ch)
	}
}

func NewRabbitMQ(conn *amqp091.Connection, max int) bus.DriverInterface {
	return &rabbitDriver{
		conn:    conn,
		master:  factory.NewMaster(max, 1),
		initMap: make(map[string]string),
		queues:  make(map[string]map[time.Duration]string),
	}
}
