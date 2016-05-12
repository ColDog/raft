package store

import (
	"github.com/boltdb/bolt"
	"log"
	"fmt"
	"encoding/binary"
	"time"
)

type BoltStore struct {
	db 	*bolt.DB
	cbs 	[]Callback
	size 	int64
	comB	[]byte
	logB	[]byte
}

func (store *BoltStore) OnCommit(cb Callback) {
	store.cbs = append(store.cbs, cb)
}

func (store *BoltStore) Append(key []byte, value []byte) error {
	return store.AppendWithStatus(key, value, 0)
}

func (store *BoltStore) Commit(key []byte) error {
	return store.setStatus(key, 1)
}

func (store *BoltStore) Abort(key []byte) error {
	return store.setStatus(key, 2)
}

func (store *BoltStore) Size()  {
	return store.size
}

func (store *BoltStore) AppendWithStatus(key []byte, value []byte, status int) error {
	store.size += 1
	err := store.db.Update(func(tx *bolt.Tx) error {
		err := tx.Bucket(store.logB).Put(key, value)
		tx.Bucket(store.comB).Put(
			key,
			append([]byte{byte(status)}, fromInt64(time.Now().UnixNano())...),
		)
		return err
	})

	if status == 1 {
		for _, cb := range store.cbs {
			cb(Entry{key, value, 1})
		}
	}

	return err
}

func (store *BoltStore) setStatus(key []byte, status int) error {
	var value []byte

	err := store.db.Update(func(tx *bolt.Tx) error {
		err := tx.Bucket(store.comB).Put(
			key,
			append([]byte{byte(status)}, fromInt64(time.Now().UnixNano())...),
		)

		v := tx.Bucket(store.logB).Get(key)
		copy(value, v)

		return err
	})

	if status == 1 {
		for _, cb := range store.cbs {
			cb(Entry{key, value, 1})
		}
	}

	return err
}

func (store *BoltStore) Next(last []byte) (Entry, bool) {
	ok := false
	var entry Entry

	key := increment(last)

	store.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket(store.logB)
		c := b.Cursor()

		for k, v := c.Seek(key); k != nil; k, v = c.Next() {
			entry = Entry{k, v, 0}
			ok = true

			t := tx.Bucket(store.comB).Get(k)

			if len(t) >= 1 {
				entry.Status = int(t[0])
			}

			copy(key, k)
			return nil
		}

		return nil
	})

	return entry, ok
}

func (store *BoltStore) NewIterator(start []byte) *Iterator {
	return &Iterator{
		store: store,
		key: start,
	}
}

func NewBoltStore(name string) *BoltStore {

	db, err := bolt.Open(name, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	store := &BoltStore{
		db: db,
		comB: []byte(fmt.Sprintf("%s-commit", name)),
		logB: []byte(fmt.Sprintf("%s-logs", name)),

	}

	store.db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(store.logB)
		tx.CreateBucketIfNotExists(store.comB)
		return nil
	})

	store.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(store.logB)

		b.ForEach(func(k, v []byte) error {
			store.Size += 1
			return nil
		})
		return nil
	})

	startKeyGenerator()

	return store
}

func fromInt64(val int64) []byte {
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, uint64(val))
	return bs
}

