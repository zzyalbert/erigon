package state

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/log"
)

// Readset is encapsulation of read set being written into files through the filter,
// rolling the files as they reach predefined size
type Readset struct {
	dir           string
	reads         map[string][]byte
	writes        map[string]int
	readSize      int
	writeSize     int
	memSize       datasize.ByteSize
	startingBlock uint64 // Starting block of the current readset
}

// NewReadset parses input arguments and creates a new readset if they are correct
func NewReadset(dir string, memsizeStr string, startingBlock uint64) (*Readset, error) {
	if memsizeStr == "" {
		return nil, fmt.Errorf("readset.size is not specified")
	}
	var rs Readset
	if err := rs.memSize.UnmarshalText([]byte(memsizeStr)); err != nil {
		return nil, fmt.Errorf("readset.size [%s] could not parsed: %v", memsizeStr, err)
	}
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("could not create directory [%s]: %v", dir, err)
	}
	rs.startingBlock = startingBlock
	rs.dir = dir
	rs.reads = make(map[string][]byte)
	rs.writes = make(map[string]int)
	return &rs, nil
}

// FinishBlock is called when a block finished processing
// forceWrite is used before closing down the process to make sure the last portion is written
func (rs *Readset) FinishBlock(block uint64, forceWrite bool) error {
	if !forceWrite && (rs.readSize+rs.writeSize) < int(rs.memSize) {
		return nil
	}
	filename := path.Join(rs.dir, fmt.Sprintf("%d-%d", rs.startingBlock, block))
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(file)
	var varintBuf [10]byte // Buffer for varint number
	for key, val := range rs.reads {
		keyLen := binary.PutUvarint(varintBuf[:], uint64(len(key)))
		if _, err = w.Write(varintBuf[:keyLen]); err != nil {
			return err
		}
		if _, err = w.Write([]byte(key)); err != nil {
			return err
		}
		valLen := binary.PutUvarint(varintBuf[:], uint64(len(val)))
		if _, err = w.Write(varintBuf[:valLen]); err != nil {
			return err
		}
		if _, err = w.Write(val); err != nil {
			return err
		}
	}
	if err = w.Flush(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	var stat os.FileInfo
	if stat, err = file.Stat(); err != nil {
		return err
	}
	log.Info("Readset flushed", "file", filename, "size", common.StorageSize(stat.Size()))
	rs.reads = make(map[string][]byte)
	rs.writes = make(map[string]int)
	rs.readSize = 0
	rs.writeSize = 0
	return nil
}

func (rs *Readset) Read(key, val []byte) {
	if _, ok := rs.reads[string(key)]; ok {
		return
	}
	rs.reads[string(key)] = val
	rs.readSize += len(key) + len(val)
}

func (rs *Readset) Write(key []byte, valLen int) {
	if oldValLen, ok := rs.writes[string(key)]; ok {
		rs.writeSize += valLen - oldValLen
	} else if val, ok := rs.reads[string(key)]; ok {
		rs.writeSize += valLen - len(val)
	} else {
		rs.writeSize += len(key) + valLen
	}
	rs.writes[string(key)] = valLen
}
