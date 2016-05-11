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

func StartKeyGenerator()  {
	go func() {
		for {
			f := <- nextChan
			lastKey = increment(lastKey)
			f.res <- lastKey
		}
	}()
}
