package mysql

import (
	"io"
)

const defaultBufSize = 4096

type buffer struct {
	buf    []byte
	rd     io.Reader
	idx    int
	length int
}

func newBuffer(rd io.Reader) buffer {
	var b [defaultBufSize]byte
	return buffer{
		buf: b[:],
		rd:  rd,
	}
}

func (b *buffer) fill(need int) error {
	n := b.length
	// move existing data to the beginning
	if n > 0 && b.idx > 0 {
		copy(b.buf[0:n], b.buf[b.idx:])
	}

	// grow buffer if necessary
	// TODO: let the buffer shrink again at some point
	//       Maybe keep the org buf slice and swap back?
	if need > len(b.buf) {
		// Round up to the next multiple of the default size
		newBuf := make([]byte, ((need/defaultBufSize)+1)*defaultBufSize)
		copy(newBuf, b.buf)
		b.buf = newBuf
	}

	b.idx = 0

	for {
		nn, err := b.rd.Read(b.buf[n:])
		n += nn

		switch err {
		case nil:
			if n < need {
				continue
			}
			b.length = n
			return nil

		case io.EOF:
			if n >= need {
				b.length = n
				return nil
			}
			return io.ErrUnexpectedEOF

		default:
			return err
		}
	}
}

func (b *buffer) readNext(need int) ([]byte, error) {
	if b.length < need {
		// refill
		if err := b.fill(need); err != nil {
			return nil, err
		}
	}

	offset := b.idx
	b.idx += need
	b.length -= need
	return b.buf[offset:b.idx], nil
}

func (b *buffer) takeSmallBuffer(length int) []byte {
	if b.length == 0 {
		return b.buf[:length]
	}
	return nil
}
