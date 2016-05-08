package msg

import (
	"log"
	"net"

	"github.com/hashicorp/yamux"
)

type Client struct {
	url 	string
	sess 	*yamux.Session
}

func (client *Client) Send(msg Message) Message {
	if client.sess.IsClosed() {
		log.Printf("attemting to reconnect")
		sess, err := newSession(client.url)
		if err != nil {
			log.Printf("client closed: %v", err)
			return NotFound
		} else {
			client.sess = sess
		}

	}
	stream, err := client.sess.Open()
	if err != nil {
		log.Printf("err sending: %v", err)
		return NewErrorMessage("connection", err)
	}

	n, err := stream.Write(msg.serialize())
	if err != nil {
		log.Printf("err sending: %v", err)
		return NewErrorMessage("connection", err)
	}

	log.Printf("sent %v, with %d bytes", msg, n)

	res := readFrom(stream)
	return ParseMessage(res)
}

func newSession(url string) (*yamux.Session, error) {
	conn, err := net.Dial("tcp4", url)
	if err != nil {
		return &yamux.Session{}, err
	}

	// Setup client side of yamux
	session, err := yamux.Client(conn, nil)
	if err != nil {
		return &yamux.Session{}, err
	}

	return session, nil
}

func NewClient(url string) (*Client, error) {
	sess, err := newSession(url)
	if err != nil {
		return &Client{}, err
	}

	return &Client{url: url, sess: sess}, nil
}
