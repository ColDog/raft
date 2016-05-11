package store

import (
	"github.com/boltdb/bolt"
)

type Iterator struct {
	key 	[]byte
}

func (it *Iterator) Next() (Entry, bool) {
	ok := false
	var entry Entry

	if it.key == nil {
		return Entry{}, false
	}

	key := increment(it.key)

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

			copy(key, k)
			return nil
		}

		return nil
	})

	it.key = key
	return entry, ok
}

func (it *Iterator) NextCount(count int) []Entry {
	entries := make([]Entry, 0)
	for i := 0; i < count; i++ {
		entry, ok := it.Next()
		if ok {
			entries = append(entries, entry)
		} else {
			break
		}
	}

	return entries
}

func NewIterator(key []byte) *Iterator {
	return &Iterator{key}
}
