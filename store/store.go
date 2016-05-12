package store


type Store interface  {
	Append(key, value []byte) error
	Commit(key []byte) error
	Abort(key []byte) error
	AppendWithStatus(key, value []byte, status int) error
	Next(last []byte) (Entry, bool)
	NewIterator(start []byte) *Iterator
	OnCommit(cb Callback)
	Size() int64
}

type Callback func(entry Entry) error
