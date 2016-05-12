package store

var nextChan chan future = make(chan future)

type future struct {
	new 	bool
	res 	chan []byte
}

func LastKey() []byte {
	c := make(chan []byte)
	nextChan <- future{false, c}
	return <- c
}

func NextKey() []byte {
	c := make(chan []byte)
	nextChan <- future{true, c}
	return <- c
}

func increment(key []byte) []byte {
	newKey := make([]byte, len(key))
	copy(newKey, key)
	l := len(newKey) - 1

	if newKey == nil || len(newKey) == 0 {
		return []byte{0}
	}

	if newKey[l] == byte(128) {
		return append(newKey, byte(0))
	} else {
		newKey[l] = byte(int(newKey[l]) + 1)
		return newKey
	}
}

func startKeyGenerator()  {
	go func() {
		lastKey := []byte{0}
		for {
			f := <- nextChan
			if f.new {
				lastKey = increment(lastKey)
			}
			f.res <- lastKey
		}
	}()
}

func keyToInt64(key []byte) int64 {
	val := int64(0)
	for _, b := range key {
		val += int64(b)
	}
	return val
}
