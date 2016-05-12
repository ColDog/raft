package store

import (
	"testing"
	"os"
	"fmt"
)

func TestAdding(t *testing.T) {
	s := NewBoltStore("test")

	id := []byte{100}

	s.Append(id, []byte("things"))

	err := s.Commit(id)
	if err != nil {
		t.Fatal(err)
	}

	id = []byte{200}
	s.Append(id, []byte("things"))

	err = s.Abort(id)
	if err != nil {
		t.Fatal(err)
	}

	it := s.NewIterator([]byte{199})

	e, _ := it.Next()
	fmt.Printf("%v\n", e)
	if e.Status != 2 {
		t.Fatal(e)
	}

	it = s.NewIterator([]byte{10})
	ee, ok := it.Next()
	fmt.Printf("ee: %v, ok: %v\n", ee, ok)
}

func TestKeyGen(t *testing.T) {
	for i := 0; i < 500; i++ {
		NextKey()
		//fmt.Printf("%v\n", NextKey())
	}
}


func init()  {
	os.Remove("test")
}
