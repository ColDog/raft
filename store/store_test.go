package store

import (
	"testing"
	"os"
	"github.com/boltdb/bolt"
	"fmt"
)

func TestAdding(t *testing.T) {

	id := int64(100)

	AppendEntry(id, []byte("things"))

	err := CommitEntry(id)
	if err != nil {
		t.Fatal(err)
	}

	printFrom(logBucket)
	printFrom(comBucket)

	id += 100
	AppendEntry(id, []byte("things"))

	err = AbortEntry(id)
	if err != nil {
		t.Fatal(err)
	}

	key, status, val := Next(101)
	println(key, status, string(val))
	if status != 2 {
		t.Fatal(status)
	}

	printFrom(logBucket)
	printFrom(comBucket)

}


func printFrom(bucket []byte) {
	fmt.Printf("\nprinting from %s\n", bucket)
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)

		b.ForEach(func(k, v []byte) error {
			fmt.Printf("key=%d, value=%v\n", toInt64(k), v)
			Size += 1
			return nil
		})
		return nil
	})
}

func init()  {
	os.Remove("test.db")
	OpenDb("test.db")
}
