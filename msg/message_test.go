package msg

import (
	"testing"
	"fmt"
)

func TestIntegers(t *testing.T) {
	i := ti(fi(1))
	if i != 1 {
		println(ti(fi(1)))
		t.Fatal("Int parse failed")
	}
}

func BenchmarkName(b *testing.B) {
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

func TestSerializer(t *testing.T) {
	m := Message{"hello", map[string] interface{} {
		"name": "COlin",
		"name3": "COlin3",
		"list": []string{"a", "b", "c"},
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

	if m2.Params["maps"].(map[string] string)["i"] != "a" {
		t.Fail()
	}

	fmt.Printf("%v\n", m2)
}
