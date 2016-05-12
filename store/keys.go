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
	if key[len(key) - 1] == byte(128) {
		return append(key, byte(0))
	} else {
		key[len(key) - 1] = byte(int(key[len(key) - 1]) + 1)
		return key
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
