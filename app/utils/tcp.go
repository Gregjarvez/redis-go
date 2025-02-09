package utils

import (
	"bufio"
	"io"
)

func Flush(w io.Writer) error {
	// If the writer is part of a *bufio.ReadWriter, flush its Writer field
	if rw, ok := w.(*bufio.ReadWriter); ok {
		return rw.Writer.Flush()
	}

	// Otherwise, check if it's a standalone *bufio.Writer and flush
	if bufWriter, ok := w.(*bufio.Writer); ok {
		return bufWriter.Flush()
	}
	return nil
}
