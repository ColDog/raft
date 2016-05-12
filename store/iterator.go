package store

type Iterator struct {
	key 	[]byte
	store 	Store
}

func (it *Iterator) Next() (Entry, bool) {
	if it.key == nil {
		return Entry{}, false
	}
	entry, ok := it.store.Next(it.key)
	it.key = entry.Key
	return entry, ok
}

func (it *Iterator) NextCount(count int) []Entry {
	entries := make([]Entry, 0)
	for i := 0; i < count; i++ {
		entry, ok := it.Next()
		if ok {
			entries = append(entries, entry)
		} else {
			break
		}
	}

	return entries
}
