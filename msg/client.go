package msg

import (
	"log"
	"net"

	"github.com/hashicorp/yamux"
)

var debug bool = false

type Client struct {
	Url 	string
	sess 	*yamux.Session
}

func (client *Client) Send(msg Message) Message {
	if client.sess.IsClosed() {
		log.Printf("attemting to reconnect")
		sess, err := newSession(client.Url)
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

	if debug {
		log.Printf("sent %v, with %d bytes", msg, n)
	}

	res := readFrom(stream)
	if res != nil && len(res) > 0 {
		return ParseMessage(res)
	} else {
		return NotFound
	}
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

	return &Client{Url: url, sess: sess}, nil
}
