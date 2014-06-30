package exp

import (
	"github.com/sorcix/irc"
	"sync"
	"time"
)

const (
	floodTime = time.Millisecond * 500 // Pause between outgoing messages
	pingTime  = time.Minute // Time between pings
)

// The Client type provides a barebones IRC client.
type Client struct {
	server *irc.Conn

	quit  chan struct{}
	queue chan *irc.Message

	handler func(*irc.Message, irc.Sender)

	quitOnce sync.Once

	wg sync.WaitGroup
}

// NewClient returns a client that communicates over conn.
//
// Incoming messages are sent to the handler func.
func NewClient(conn *irc.Conn, handler func(*irc.Message, irc.Sender)) *Client {

	if conn == nil || handler == nil {
		// An IRC client is useless without a connection or a handler.
		return nil
	}

	c := new(Client)
	c.quit = make(chan struct{})
	c.queue = make(chan *irc.Message, 100)

	c.handler = handler
	c.server = conn

	c.wg.Add(3)

	go c.input()
	go c.output()
	go c.ping()

	return c
}

// Identify sends both USER and NICK messages to the server.
//
// On servers without a password, this should be the first thing to do!
func (c *Client) Identify(nickname, username, realname string) {
	c.Send(&irc.Message{
		Command:  irc.USER,
		Params:   []string{username, "0", "*"},
		Trailing: realname,
	})
	c.Send(&irc.Message{
		Command: irc.NICK,
		Params:  []string{nickname},
	})
}

// Sends queues a message for sending.
//
// Messages are queued to prevent flooding.
func (c *Client) Send(m *irc.Message) error {
	c.queue <- m
	return nil
}

// Disconnect stops all goroutines and closes the underlying connection.
func (c *Client) Disconnect() {
	c.server.Close()
	c.quitOnce.Do(func() {
		close(c.quit)
	})
}

// Wait blocks until the client exited.
func (c *Client) Wait() {
	c.wg.Wait()
}

// ping keeps the connection alive by sending a ping every minute.
func (c *Client) ping() {

	defer c.wg.Done()

	ticker := time.NewTicker(pingTime)
	for {
		select {

		case <-c.quit:
			ticker.Stop()
			return

		case <-ticker.C:
			c.Send(&irc.Message{
				Command:  irc.PING,
				Trailing: time.Now().Format(time.RFC3339Nano),
			})
		}
	}
}

// input reads messages from the server and passes them to the handler.
func (c *Client) input() {

	var (
		m   *irc.Message
		err error
	)

	defer c.wg.Done()

	for {
		select {

		case <-c.quit:
			return

		default:
			if m, err = c.server.Decoder.Decode(); err != nil {
				c.Disconnect()
				return
			}
			go c.handler(m, c)

		}
	}
}

// output consumes messages from the sending queue and sends them to the server.
func (c *Client) output() {

	var (
		m *irc.Message
		err error
	)

	defer c.wg.Done()

	for {
		select {

		case <-c.quit:
			return

		case m = <-c.queue:
			if err = c.server.Encoder.Encode(m); err != nil {
				c.Disconnect()
				return
			}
			time.Sleep(floodTime)
		}
	}
}
