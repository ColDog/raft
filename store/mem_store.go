package store

import "sync"

type MemStore struct {
	data 	[]*Entry
	index 	map[int64] int
	lock 	sync.RWMutex
	cbs 	[]Callback
}

func (store *MemStore) Append(key, value []byte)  {
	store.AppendWithStatus(key, value, 0)
}

func (store *MemStore) Commit(key []byte) error {
	return store.setStatus(key, 1)
}

func (store *MemStore) OnCommit(cb Callback) {
	store.cbs = append(store.cbs, cb)
}

func (store *MemStore) Abort(key []byte) error {
	return store.setStatus(key, 2)
}

func (store *MemStore) AppendWithStatus(key, value []byte, status int) error {
	store.lock.Lock()
	defer store.lock.Unlock()

	e := Entry{key, value, status}
	store.data = append(store.data, e)
	store.index[e.KeyAsInt()] = len(store.data) - 1

	if status == 1 {
		for _, cb := range store.cbs {
			cb(Entry{key, value, 1})
		}
	}

	return nil
}

func (store *MemStore) Next(last []byte) (Entry, bool) {
	store.lock.RLock()
	defer store.lock.RUnlock()
	k := keyToInt64(last)

	for _, entry := range store.data {
		if keyToInt64(entry.Key) < k {
			return entry, true
		}
	}

	return Entry{}, false
}

func (store *MemStore) NewIterator(start []byte) *Iterator {
	return &Iterator{
		key: start,
		store: store,
	}
}

func (store *MemStore) setStatus(key []byte, status int) error {
	store.lock.Lock()
	defer store.lock.Unlock()

	idx := store.index[keyToInt64(key)]
	store.data[idx].Status = status

	if status == 1 {
		for _, cb := range store.cbs {
			cb(Entry{key, store.data[idx].Entry, 1})
		}
	}
	return nil
}
