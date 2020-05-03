package queue

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/haraqa/haraqa/internal/zeroc"
)

const (
	datFileExt = ".dat"
	hrqFileExt = ".hrq"

	datumLength = 24
)

//go:generate mockgen -source files.go -destination ../mocks/file.go -package mocks
//go:generate mockgen -package mocks -destination ../mocks/file_info.go os FileInfo

// File is an interface covering os.File operations used by the queue
type File interface {
	io.ReadWriteCloser
	Stat() (os.FileInfo, error)
	ReadAt([]byte, int64) (int, error)
}

func formatFilename(n int64) string {
	return fmt.Sprintf("%016d", n)
}

type datum [datumLength]byte

func (d *datum) GetOffsetID() int64 {
	return int64(binary.BigEndian.Uint64(d[:8]))
}

func (d *datum) SetOffsetID(id int64) *datum {
	binary.BigEndian.PutUint64(d[:8], uint64(id))
	return d
}

func (d *datum) GetStartAt() int64 {
	return int64(binary.BigEndian.Uint64(d[8:16]))
}

func (d *datum) SetStartAt(n int64) *datum {
	binary.BigEndian.PutUint64(d[8:16], uint64(n))
	return d
}

func (d *datum) GetMsgSize() int64 {
	return int64(binary.BigEndian.Uint64(d[16:24]))
}

func (d *datum) SetMsgSize(n int64) *datum {
	binary.BigEndian.PutUint64(d[16:24], uint64(n))
	return d
}

func writeDat(mw *zeroc.MultiWriter, offset int64, startAt int64, msgSizes []int64) error {
	buf := make([]byte, 24*len(msgSizes))
	var dat datum
	for i := range msgSizes {
		dat.SetOffsetID(offset).SetStartAt(startAt).SetMsgSize(msgSizes[i])
		copy(buf[24*i:24*(i+1)], dat[:])
		startAt += msgSizes[i]
		offset++
	}

	_, err := mw.Write(buf[:])
	if err != nil {
		return err
	}

	mw.IncreaseOffset(int64(len(buf)))

	return nil
}

func readDat(f File, offset int64, limit int64) (int64, []int64, error) {
	defer f.Close()

	var fileSize int64
	if offset < 0 || limit < 0 {
		info, err := f.Stat()
		if err != nil {
			return 0, nil, err
		}
		fileSize = info.Size()
		if fileSize < datumLength {
			return 0, nil, nil
		}
	}

	// if the offset is negative (get the latest message), find the latest message offset
	if offset < 0 {
		offset = fileSize/datumLength - 1
	}

	// if limit is less than 1 set to dat size
	if limit < 1 {
		limit = fileSize/datumLength + 1
	}

	// read the dat file
	buf := make([]byte, limit*datumLength)
	n, err := f.ReadAt(buf[:], offset*datumLength)
	if err != nil && err != io.EOF {
		return 0, nil, err
	}
	if n < datumLength {
		return 0, nil, nil
	}
	buf = buf[:n-n%datumLength]

	startAt := int64(binary.BigEndian.Uint64(buf[8:16]))
	msgSizes := make([]int64, 0, n/datumLength)
	for i := 0; i < len(buf); i += datumLength {
		msgSizes = append(msgSizes, int64(binary.BigEndian.Uint64(buf[i+16:i+24])))
	}

	return startAt, msgSizes, nil
}
