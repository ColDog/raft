package main

import (
	"github.com/boltdb/bolt"
	"log"
)

var db *bolt.DB
var logBucket []byte = []byte("logs")
var comBucket []byte = []byte("commits")

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
}
