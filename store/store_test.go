package store

import (
	"testing"
	"os"
	"github.com/boltdb/bolt"
	"fmt"
)

func TestAdding(t *testing.T) {

	id := []byte{100}

	AppendEntry(id, []byte("things"))

	err := CommitEntry(id)
	if err != nil {
		t.Fatal(err)
	}

	printFrom(logBucket)
	printFrom(comBucket)

	id = []byte{200}
	AppendEntry(id, []byte("things"))

	err = AbortEntry(id)
	if err != nil {
		t.Fatal(err)
	}

	it := NewIterator([]byte{199})

	e, _ := it.Next()
	fmt.Printf("%v\n", e)
	if e.Status != 2 {
		t.Fatal(e)
	}

	printFrom(logBucket)
	printFrom(comBucket)

	it = NewIterator([]byte{10})
	ee, ok := it.Next()
	fmt.Printf("ee: %v, ok: %v\n", ee, ok)
}

func TestIterator(t *testing.T) {
	println("\n\nentries")
	it := NewIterator([]byte{0})
	es := it.NextCount(100)

	printFrom(logBucket)

	fmt.Printf("entries: %v size: %v\n", len(es), Size)
}

func TestKeyGen(t *testing.T) {
	for i := 0; i < 500; i++ {
		NextKey()
		//fmt.Printf("%v\n", NextKey())
	}
}

func printFrom(bucket []byte) {
	fmt.Printf("\nprinting from %s\n", bucket)
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)

		b.ForEach(func(k, v []byte) error {
			fmt.Printf("key=%d, value=%v\n", k, v)
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
