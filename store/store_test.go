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

	e, _ := Next([]byte{200})
	fmt.Printf("%v\n", e)
	if e.Status != 2 {
		t.Fatal(e)
	}

	printFrom(logBucket)
	printFrom(comBucket)


	ee, ok := Next([]byte{10})
	fmt.Printf("ee: %v, ok: %v\n", ee, ok)
}

func TestIncrementBytes(t *testing.T) {
	k0 := []byte{100, 128}
	fmt.Printf("incremented: %v\n", increment(k0))
}

func TestKeyGen(t *testing.T) {
	StartKeyGenerator()

	go fmt.Printf("key: %v %v\n", NextKey(), LastKey())
	go fmt.Printf("key: %v %v\n", NextKey(), LastKey())
	fmt.Printf("key: %v %v\n", NextKey(), LastKey())
	fmt.Printf("key: %v %v\n", NextKey(), LastKey())
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
