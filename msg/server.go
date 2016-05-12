package msg

import (
	"io"
	"log"
	"net"

	"github.com/hashicorp/yamux"
)

type Handler func(m Message) Message

var handlers map[string] Handler = make(map[string] Handler)

func Handle(action string, handler Handler)  {
	log.Printf("[msg] adding handler %v", action)
	handlers[action] = handler
}

func readFrom(sconn net.Conn) []byte {
	var n int
	var err error
	buff := make([]byte, 0xff)
	for {
		n, err = sconn.Read(buff)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Printf("Stream read error: %s", err)
			break
		}


		return buff[:n]
	}

	return nil
}

func stream(sconn net.Conn) {
	req := readFrom(sconn)

	msg := ParseMessage(req)

	if debug {
		log.Printf("received %v", msg)
	}

	if handler, ok := handlers[msg.action]; ok {
		res := handler(msg)
		sconn.Write(res.serialize())
	} else {
		log.Printf("no handler found %s", msg.action)
	}
}

func handle(conn net.Conn) {

	log.Printf("TCP accepted")

	// Setup server side of yamux
	session, err := yamux.Server(conn, nil)
	if err != nil {
		log.Printf("Yamux server: %s", err)
	}

	for {
		sconn, err := session.Accept()
		if err != nil {
			if session.IsClosed() {
				log.Printf("TCP closed")
				break
			}
			log.Printf("Yamux accept: %s", err)
			continue
		}
		go stream(sconn)
	}
}

func Serve(url string) {
	log.Println("starting tcp server")

	l, err := net.Listen("tcp4", url)
	if err != nil {
		log.Printf("TCP server: %s", err)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("TCP accept: %s", err)
		}
		go handle(conn)
	}

}
