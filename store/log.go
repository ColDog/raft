package store

import (
	"github.com/boltdb/bolt"
	"log"
	"bytes"
	"encoding/binary"
	"fmt"
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

func Next(last int64) (int64, []byte) {
	last += 1
	var rkey int64
	var rval []byte

	key := fromInt64(last)

	db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket(logBucket)
		c := b.Cursor()

		for k, v := c.Seek(key); k != nil; k, v = c.Next() {
			rkey = toInt64(k)
			copy(rval, v)
			return nil
		}

		return nil
	})

	return rkey, rval
}

func AppendEntry(id int64, value []byte)  {
	LastKey = id
	Size += 1
	Append(fromInt64(id), value)
}

func CommitEntry(id int64)  {
	LastKey = id
	Commit(fromInt64(id))
}

func AbortEntry(id int64)  {
	LastKey = id
	Abort(fromInt64(id))
}

func Append(key []byte, value []byte) error {
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucket)
		err := b.Put(key, value)
		return err
	})

	return err
}

func Commit(key []byte) error  {
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(comBucket)
		err := b.Put(key, []byte{1})
		return err
	})

	return err
}

func Abort(key []byte) error  {
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(comBucket)
		err := b.Put(key, []byte{0})
		return err
	})

	return err
}

func Read(key []byte) ([]byte, bool) {
	committed := false
	var value []byte

	db.View(func(tx *bolt.Tx) error {
		commit := tx.Bucket(comBucket).Get(key)
		if commit != nil && len(commit) >= 1 && commit[0] == 1 {
			committed = true
		}

		copy(value, tx.Bucket(logBucket).Get(key))
		return nil
	})

	return value, committed
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

	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucket)

		b.ForEach(func(k, v []byte) error {
			fmt.Printf("key=%s, value=%s\n", k, v)
			Size += 1
			return nil
		})
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

