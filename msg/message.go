package msg

import (
	"bytes"
	"encoding/binary"
)

// This message is a very lightweight binary serialization protocol that stores messages in the following format:
// [ action len ][ action name ][  key len  ][  val len  ][  key  ][  val  ] repeat key val
//   (4 bytes)         ?         (4 bytes)    (4 bytes)      ?        ?
//
// It allows for very fast serialization and deserialization of messages without too much overhead. Messages are simply
// just byte arrays. A basic map is used to hold the message body where key's are strings only. Message values are either
// integers, strings, bytes or byte arrays. They are encoded into a generic interface{}.
//
// The message also specifies an 'action'. This is a key used to route to different functions during rpc calls and in
// pub/sub calls this is the 'topic' of the message.
//
// For encoding and decoding values, a byte is added at the start of the value which marks its type:
// 1: int
// 2: string
// 3: byte
// 4: byte array
// 5: int64

var NotFound Message = Message{"errors.not_found", make(map[string] interface{}), true}
var ServerError Message = Message{"errors.server_error", make(map[string] interface{}), true}
var BadRequest Message = Message{"errors.bad_request", make(map[string] interface{}), true}

var Ack Message = Message{"ack", make(map[string] interface{}), false}
var Ping Message = Message{"ping", make(map[string] interface{}), false}

func NewMessage(action string) Message {
	return Message{action, make(map[string] interface{}), false}
}

func NewErrorMessage(key string, err error) Message {
	return Message{"errors." + key, map[string] interface{} {"error": err.Error()}, true}
}

func NewKvMessage(action string, key string, value interface{}) Message {
	msg := NewMessage(action)
	msg.Params[key] = value
	msg.failed = false
	return msg
}

func ParseMessage(msg []byte) Message {
	l := ti(msg[0:4])
	action := string(msg[4:4 + l])
	params := make(map[string] interface{})

	for i := 4 + l; i < len(msg); {
		kl := ti(msg[i:i + 4])
		vl := ti(msg[i + 4:i + 8])

		key := string(msg[i + 8:i + 8 + kl])
		val := msg[i + 8 + kl:i + 8 + kl + vl]

		switch val[0] {
		case byte(1):
			params[key] = ti(val[1:])
		case byte(2):
			params[key] = string(val[1:])
		case byte(3):
			params[key] = val[1]
		case byte(4):
			params[key] = val[1:]
		case byte(5):
			params[key] = toInt64(val[1:])
		case byte(6):
			ary := make([]string, 0)
			for si := 1; si < len(val); {
				l := ti(val[si:si + 4])
				item := string(val[si + 4:si + 4 + l])
				ary = append(ary, item)
				si = si + 4 + l
			}
			params[key] = ary
		case byte(7):
			res := make(map[string] string)
			curr := ""
			for si := 1; si < len(val); {
				l := ti(val[si:si + 4])
				item := string(val[si + 4:si + 4 + l])
				if curr == "" {
					curr = item
				} else {
					res[curr] = item
					curr = ""
				}
				si = si + 4 + l
			}
			params[key] = res
		case byte(8):
			ary := make([][]byte, 0)
			for si := 1; si < len(val); {
				l := ti(val[si:si + 4])
				item := val[si + 4:si + 4 + l]
				ary = append(ary, item)
				si = si + 4 + l
			}
			params[key] = ary
		case byte(9):
			ary := make([]int64, 0)
			for si := 1; si < len(val); {
				ary = append(ary, toInt64(val[si:si + 8]))
				si = si + 8
			}
			params[key] = ary
		}


		i = i + 8 + kl + vl

	}

	return Message{action, params, false}
}

type Message struct {
	action 	string
	Params 	map[string] interface{}
	failed 	bool
}

func (msg Message) MarkFailed() {
	msg.failed = true
}

func (msg Message) Name() string {
	return msg.action
}

func (msg Message) Failed() bool {
	return msg.failed
}

func (msg Message) serialize() []byte {
	bact := []byte(msg.action)

	buff := new(bytes.Buffer)
	buff.Write(fi(len(bact)))
	buff.Write(bact)

	for key, val := range msg.Params {
		bkey := []byte(key)
		var bval []byte

		switch asserted := val.(type) {
		case int:
			bval = append([]byte{1}, fi(asserted)...)
		case string:
			bval = append([]byte{2}, []byte(asserted)...)
		case byte:
			bval = []byte{byte(3), asserted}
		case []byte:
			bval = append([]byte{4}, asserted...)
		case int64:
			bval = append([]byte{5}, fromInt64(asserted)...)
		case []string:
			// string array:
			// [6][ item len ][ item ] ++
			//  1      4          ?
			stAry := new(bytes.Buffer)
			stAry.Write([]byte{6})
			for _, item := range asserted {
				b := []byte(item)
				stAry.Write(fi(len(b)))
				stAry.Write(b)
			}
			bval = stAry.Bytes()
		case map[string] string:
			// map of strings, serialize to a list, one key before the other
			stAry := new(bytes.Buffer)
			stAry.Write([]byte{7})

			for key, val := range asserted {
				b1 := []byte(key)
				stAry.Write(fi(len(b1)))
				stAry.Write(b1)

				b2 := []byte(val)
				stAry.Write(fi(len(b2)))
				stAry.Write(b2)
			}
			bval = stAry.Bytes()
		case [][]byte:
			// byte x 2 array:
			// [8][ item len ][ item ] ++
			//  1      4          ?
			bAry := new(bytes.Buffer)
			bAry.Write([]byte{8})
			for _, item := range asserted {
				bAry.Write(fi(len(item)))
				bAry.Write(item)
			}
			bval = bAry.Bytes()
		case []int64:
			bAry := new(bytes.Buffer)
			bAry.Write([]byte{9})
			for _, item := range asserted {
				bAry.Write(fromInt64(item))
			}
			bval = bAry.Bytes()
		}

		buff.Write(fi(len(bkey)))
		buff.Write(fi(len(bval)))
		buff.Write(bkey)
		buff.Write(bval)
	}

	return buff.Bytes()
}

func fromInt64(val int64) []byte {
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, uint64(val))
	return bs
}

func toInt64(data []byte) int64 {
	var value uint64
	buf := bytes.NewReader(data)
	binary.Read(buf, binary.LittleEndian, &value)
	return int64(value)
}

func fi(arg int) []byte {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(arg))
	return bs
}

func ti(data []byte) int {
	var value uint32
	buf := bytes.NewReader(data)
	binary.Read(buf, binary.LittleEndian, &value)
	return int(value)
}
