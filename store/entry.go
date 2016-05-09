package store

type Entry struct {
	Id 	int64
	Entry	[]byte
}

func (entry Entry) Commit() {
	CommitEntry(entry.Id)
}

func (entry Entry) Append() {
	AppendEntry(entry.Id, entry.Entry)
}

func (entry Entry) Abort() {
	AbortEntry(entry.Id)
}

func NewEntry(entry []byte) Entry {
	return Entry{NextKey(), entry}
}

