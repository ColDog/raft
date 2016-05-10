package store

import (
	"github.com/boltdb/bolt"

	"log"
	"bytes"
	"encoding/binary"
	"time"
	"math/rand"
)

var db *bolt.DB
var logBucket []byte = []byte("logs")
var comBucket []byte = []byte("commits")
var LastKey int64 = 0
var Size int64 = 0

func NextKey() int64 {
	t := time.Now().UnixNano()
	if t > LastKey {
		return t
	} else {
		return LastKey + rand.Int63n(10000)
	}
}

func Next(last int64) (int64, int, []byte) {
	var rkey int64
	var rval []byte
	var rstat int

	key := fromInt64(last)

	db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket(logBucket)
		c := b.Cursor()

		for k, v := c.Seek(key); k != nil; k, v = c.Next() {
			rkey = toInt64(k)
			rval = v

			t := tx.Bucket(comBucket).Get(k)

			if len(t) >= 1 {
				rstat = int(t[0])
			}

			return nil
		}

		return nil
	})

	return rkey, rstat, rval
}

func AppendEntry(id int64, value []byte) error {
	LastKey = id
	Size += 1
	key := fromInt64(id)

	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucket)
		err := b.Put(key, value)
		return err
	})

	return err
}

func CommitEntry(id int64) error {
	return SetEntryStatus(id, 1)
}

func AbortEntry(id int64) error {
	return SetEntryStatus(id, 2)
}

func SetEntryStatus(id int64, status int) error {
	key := fromInt64(id)
	err := db.Update(func(tx *bolt.Tx) error {
		ts := fromInt64(time.Now().UnixNano())
		err := tx.Bucket(comBucket).Put(key, append([]byte{byte(status)}, ts...))
		return err
	})

	return err
}


func OpenDb(name string) {
	var err error
	db, err = bolt.Open(name, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(logBucket)
		tx.CreateBucketIfNotExists(comBucket)
		return nil
	})
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

