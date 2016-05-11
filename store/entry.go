package store

type Entry struct {
	Key 	[]byte
	Entry	[]byte
	Status 	int
}

func (entry Entry) Commit() {
	CommitEntry(entry.Key)
}

func (entry Entry) Append() {
	if entry.Status > 0 {
		AppendEntryWithStatus(entry.Key, entry.Entry, entry.Status)
	} else {
		AppendEntry(entry.Key, entry.Entry)
	}
}

func (entry Entry) Abort() {
	AbortEntry(entry.Key)
}

func (entry Entry) KeyAsInt() int64 {
	return keyToInt64(entry.Key)
}

func NewEntry(entry []byte) Entry {
	return Entry{NextKey(), entry, 0}
}

