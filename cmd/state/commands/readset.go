package commands

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"os"
	"path"

	"github.com/c2h5oh/datasize"
)

// Readset is encapsulation of read set being written into files through the filter,
// rolling the files as they reach predefined size
type Readset struct {
	dir            string
	varintBuf      [10]byte    // Buffer for varint number
	hasherBuf      [4]byte     // Hasher buffer
	hasher         hash.Hash32 // Hash for the filter
	filter         []uint64
	filesize       datasize.ByteSize
	currentFile    *os.File
	currentWriter  *bufio.Writer
	currentWritten int    // Number of bytes written into the current file so far
	startingBlock  uint64 // Starting block of the current readset
}

// NewReadset parses input arguments and creates a new readset if they are correct
func NewReadset(dir string, filesizeStr string, filtersizeStr string, startingBlock uint64) (*Readset, error) {
	if filesizeStr == "" {
		return nil, fmt.Errorf("filesize is not specified")
	}
	var rs Readset
	if err := rs.filesize.UnmarshalText([]byte(filesizeStr)); err != nil {
		return nil, fmt.Errorf("filesize [%s] could not parsed: %v", filesizeStr, err)
	}
	if filtersizeStr == "" {
		return nil, fmt.Errorf("filtersize is not specified")
	}
	var filtersize datasize.ByteSize
	if err := filtersize.UnmarshalText([]byte(filtersizeStr)); err != nil {
		return nil, fmt.Errorf("filtersize [%s] could not parsed: %v", filtersizeStr, err)
	}
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("could not create directory [%s]: %v", dir, err)
	}
	rs.startingBlock = startingBlock
	rs.filter = make([]uint64, (int(filtersize)+7)/8)
	rs.dir = dir
	rs.hasher = fnv.New32a()
	return &rs, nil
}

// ensureFile is an internal function that creates a new current file if required
func (rs *Readset) ensureFile() error {
	if rs.currentFile != nil {
		return nil
	}
	var err error
	if rs.currentFile, err = os.Create(path.Join(rs.dir, "current")); err != nil {
		return fmt.Errorf("could not create current file: %v", err)
	}
	rs.currentWriter = bufio.NewWriter(rs.currentFile)
	return nil
}

// FinishBlock is called when a block finished processing
// forceWrite is used before closing down the process to make sure the last portion is written
func (rs *Readset) FinishBlock(block uint64, forceWrite bool) error {
	if !forceWrite && rs.currentWritten < int(rs.filesize) {
		return nil
	}
	if rs.currentFile == nil {
		// No data was written since last time
		return nil
	}
	if err := rs.currentWriter.Flush(); err != nil {
		return err
	}
	if err := rs.currentFile.Close(); err != nil {
		return err
	}
	os.Rename(path.Join(rs.dir, "current"), path.Join(rs.dir, fmt.Sprintf("%d-%d", rs.startingBlock, block)))
	rs.currentFile = nil
	rs.startingBlock = block + 1
	// Clean the filter
	for i := 0; i < len(rs.filter); i++ {
		rs.filter[i] = 0
	}
	return nil
}

func (rs *Readset) Write(key, value []byte) error {
	if err := rs.ensureFile(); err != nil {
		return err
	}
	rs.hasher.Write(key)
	rs.hasher.Sum(rs.hasherBuf[:])
	idxUint32 := binary.BigEndian.Uint32(rs.hasherBuf[:])
	idx := idxUint32 % uint32(8*len(rs.filter))
	hit := rs.filter[idx/64] & (uint64(1) << (idx & 63))
	if 
	keyLen := binary.PutUvarint(rs.varintBuf[:], uint64(len(key)))
	return nil
}
