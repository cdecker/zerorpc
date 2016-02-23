// Package zerorpc provides a client/server Golang library for the ZeroRPC protocol,
//
// for additional info see http://zerorpc.dotcloud.com
package zerorpc

import (
	"sync"

	zmq "github.com/pebbe/zmq4"
)

// ZeroRPC socket representation
type socket struct {
	zmqSocket    *zmq.Socket
	Channels     []*channel
	server       *Server
	socketErrors chan error
	mu           sync.Mutex
}

// Connects to a ZeroMQ endpoint and returns a pointer to a new znq.DEALER socket,
// a listener for incoming messages is invoked on the new socket
func connect(endpoint string) (*socket, error) {
	zmqSocket, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		return nil, err
	}

	s := socket{
		zmqSocket:    zmqSocket,
		Channels:     make([]*channel, 0),
		socketErrors: make(chan error),
	}

	if err := s.zmqSocket.Connect(endpoint); err != nil {
		return nil, err
	}

	go s.listen()

	return &s, nil
}

// Binds to a ZeroMQ endpoint and returns a pointer to a new znq.ROUTER socket,
// a listener for incoming messages is invoked on the new socket
func bind(endpoint string) (*socket, error) {
	zmqSocket, err := zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		return nil, err
	}

	s := socket{
		zmqSocket: zmqSocket,
		Channels:  make([]*channel, 0),
	}

	if err := s.zmqSocket.Bind(endpoint); err != nil {
		return nil, err
	}

	go s.listen()

	return &s, nil
}

// Close the socket,
// it closes all the channels first
func (s *socket) close() error {
	for _, c := range s.Channels {
		c.close()
		s.removeChannel(c)
	}

	return s.zmqSocket.Close()
}

// Removes a channel from the socket's array of channels
func (s *socket) removeChannel(c *channel) {
	s.mu.Lock()
	defer s.mu.Unlock()

	channels := make([]*channel, 0)

	for _, t := range s.Channels {
		if t != c {
			channels = append(channels, t)
		}
	}

	s.Channels = channels
}

// Sends an event on the ZeroMQ socket
func (s *socket) sendEvent(e *Event, identity string) error {
	b, err := e.packBytes()
	if err != nil {
		return err
	}

	_, err = s.zmqSocket.SendMessage(identity, "", b)
	if err != nil {
		return err
	}

	return nil
}

func (s *socket) listen() {
	for {
		barr, err := s.zmqSocket.RecvMessageBytes(0)
		if err != nil {
			s.socketErrors <- err
		}

		t := 0
		for _, k := range barr {
			t += len(k)
		}

		ev, err := unPackBytes(barr[len(barr)-1])
		if err != nil {
			s.socketErrors <- err
		}

		var ch *channel
		if _, ok := ev.Header["response_to"]; !ok {
			ch = s.newChannel(ev.Header["message_id"].(string))
			go ch.sendHeartbeats()

			if len(barr) > 1 {
				ch.identity = string(barr[0])
			}
		} else {
			for _, c := range s.Channels {
				if c.Id == ev.Header["response_to"].(string) {
					ch = c
				}
			}
		}

		if ch != nil && ch.state == open {
			ch.socketInput <- ev
		}
	}
}
