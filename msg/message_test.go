package msg

import (
	"testing"
	"fmt"
	"gopkg.in/vmihailenco/msgpack.v2"
)

func TestIntegers(t *testing.T) {
	i := ti(fi(1))
	if i != 1 {
		println(ti(fi(1)))
		t.Fatal("Int parse failed")
	}
}

func BenchmarkMsgPackSerialization(b *testing.B) {
	msg := map[string] interface{} {
		"name": "COlin",
		"name3": "COlin3",
		"list": []string{"a", "b", "c"},
		"maps": map[string] string {
			"i": "a",
			"longers": "asdfasdf",
		},
	}

	for i := 0; i < b.N; i++ {
		_, err := msgpack.Marshal(msg)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkMsgPackDeserialization(b *testing.B) {
	msg := map[string] interface{} {
		"name": "COlin",
		"name3": "COlin3",
		"list": []string{"a", "b", "c"},
		"maps": map[string] string {
			"i": "a",
			"longers": "asdfasdf",
		},
	}

	data, err := msgpack.Marshal(msg)
	if err != nil {
		panic(err)
	}

	for i := 0; i < b.N; i++ {
		var out map[string] interface{}
		err = msgpack.Unmarshal(data, &out)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkMySerialization(b *testing.B) {
	m := Message{"hello", map[string] interface{} {
		"name": "COlin",
		"name3": "COlin3",
		"list": []string{"a", "b", "c"},
		"maps": map[string] string {
			"i": "a",
			"longers": "asdfasdf",
		},
	}, true}

	for i := 0; i < b.N; i++ {
		m.serialize()
	}
}

func BenchmarkMyDeserialization(b *testing.B) {
	m := Message{"hello", map[string] interface{} {
		"name": "COlin",
		"name3": "COlin3",
		"list": []string{"a", "b", "c"},
		"maps": map[string] string {
			"i": "a",
			"longers": "asdfasdf",
		},
	}, true}

	serialized := m.serialize()

	for i := 0; i < b.N; i++ {
		ParseMessage(serialized)
	}
}

func TestSerializer(t *testing.T) {
	m := Message{"hello", map[string] interface{} {
		"name": "COlin",
		"name3": "COlin3",
		"list": []string{"a", "b", "c"},
		"intList": []int64{1, 2, 3, 4},
		"maps": map[string] string {
			"i": "a",
			"longers": "asdfasdf",
		},
	}, true}

	m2 := ParseMessage(m.serialize())

	if m2.action != m.action {
		t.Fail()
	}

	if m2.Params["name"] != m.Params["name"] {
		t.Fail()
	}

	if m2.Params["name3"] != m.Params["name3"] {
		t.Fail()
	}

	for idx, item := range m.Params["list"].([]string) {
		if item != m2.Params["list"].([]string)[idx] {
			t.Fail()
		}
	}

	for idx, item := range m.Params["intList"].([]int64) {
		if item != m2.Params["intList"].([]int64)[idx] {
			t.Fatal(m2.Params["intList"].([]int64))
		}
	}

	if m2.Params["maps"].(map[string] string)["i"] != "a" {
		t.Fail()
	}

	fmt.Printf("%v\n", m2)
}
