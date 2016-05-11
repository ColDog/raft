package store

import (
	"github.com/boltdb/bolt"

	"log"
	"bytes"
	"encoding/binary"
	"time"
)

var db *bolt.DB
var logBucket []byte = []byte("logs")
var comBucket []byte = []byte("commits")
var Size int64 = 0

func Next(key []byte) (Entry, bool) {
	ok := false
	var entry Entry

	db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket(logBucket)
		c := b.Cursor()

		for k, v := c.Seek(key); k != nil; k, v = c.Next() {
			entry = Entry{k, v, 0}
			ok = true

			t := tx.Bucket(comBucket).Get(k)

			if len(t) >= 1 {
				entry.Status = int(t[0])
			}

			return nil
		}

		return nil
	})

	return entry, ok
}

func AppendEntry(key []byte, value []byte) error {
	return AppendEntryWithStatus(key, value, 0)
}

func CommitEntry(key []byte) error {
	return SetEntryStatus(key, 1)
}

func AbortEntry(key []byte) error {
	return SetEntryStatus(key, 2)
}

func AppendEntryWithStatus(key []byte, value []byte, status int) error {
	Size += 1

	err := db.Update(func(tx *bolt.Tx) error {
		err := tx.Bucket(logBucket).Put(key, value)
		tx.Bucket(comBucket).Put(
			key,
			append([]byte{byte(status)}, fromInt64(time.Now().UnixNano())...),
		)
		return err
	})

	return err
}

func SetEntryStatus(key []byte, status int) error {
	err := db.Update(func(tx *bolt.Tx) error {
		err := tx.Bucket(comBucket).Put(
			key,
			append([]byte{byte(status)}, fromInt64(time.Now().UnixNano())...),
		)
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

	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucket)

		b.ForEach(func(k, v []byte) error {
			Size += 1
			lastKey = k
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
