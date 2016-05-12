package store

type Entry struct {
	Key 	[]byte
	Entry	[]byte
	Status 	int
}

func (entry Entry) KeyAsInt() int64 {
	return keyToInt64(entry.Key)
}

func NewEntry(entry []byte) Entry {
	return Entry{NextKey(), entry, 0}
}

