package resp

type ByteCounter struct {
	size int
}

func (bc *ByteCounter) Write(b []byte) (int, error) {
	c := len(b)
	bc.size += c
	return c, nil
}
