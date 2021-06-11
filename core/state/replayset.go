package state

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
)

type Replayset struct {
	cache      map[string][]byte
	startBlock uint64
	endBlock   uint64
}

func NewReplayset(filepath string) (*Replayset, error) {
	base := strings.Split(path.Base(filepath), "-")
	var r Replayset
	var err error
	if r.startBlock, err = strconv.ParseUint(base[0], 10, 64); err != nil {
		return nil, err
	}
	if r.endBlock, err = strconv.ParseUint(base[1], 10, 64); err != nil {
		return nil, err
	}
	r.cache = make(map[string][]byte)
	var f *os.File
	if f, err = os.Open(filepath); err != nil {
		return nil, err
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	var keyLen uint64
	for keyLen, err = binary.ReadUvarint(reader); err == nil; keyLen, err = binary.ReadUvarint(reader) {
		key := make([]byte, int(keyLen))
		if _, err = io.ReadFull(reader, key); err != nil {
			return nil, err
		}
		var valLen uint64
		if valLen, err = binary.ReadUvarint(reader); err != nil {
			return nil, err
		}
		val := make([]byte, int(valLen))
		if valLen > 0 {
			if _, err = io.ReadFull(reader, val); err != nil {
				return nil, err
			}
		}
		r.cache[string(key)] = val
	}
	return &r, nil
}

func (r *Replayset) Read(key []byte) ([]byte, error) {
	if val, ok := r.cache[string(key)]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("unexpected read %x", key)
}

func (r *Replayset) Write(key, val []byte) {
	r.cache[string(key)] = val
}

func (r *Replayset) StartBlock() uint64 {
	return r.startBlock
}

func (r *Replayset) EndBlock() uint64 {
	return r.endBlock
}
