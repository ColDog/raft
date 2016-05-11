package store

var lastKey []byte = []byte{0}
var nextChan chan future = make(chan future)

type future struct {
	res 	chan []byte
}

func LastKey() []byte {
	return lastKey
}

func NextKey() []byte {
	c := make(chan []byte)
	nextChan <- future{c}
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
		for {
			f := <- nextChan
			lastKey = increment(lastKey)
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
