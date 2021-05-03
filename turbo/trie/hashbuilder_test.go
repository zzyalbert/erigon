package trie

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/turbo/rlphacks"
	"github.com/stretchr/testify/assert"
)

func retain(_ []byte) bool {
	return false
}

func TestEmbeddedBranchNode(t *testing.T) {
	// This trie contains a small branch node whose RLP is shorter than 32 bytes.
	key1 := common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000001")
	key2 := common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000002")

	val1 := common.FromHex("0x01")
	val2 := common.FromHex("0x02")

	hb := NewHashBuilder(false)
	hb.Reset()

	var leafData GenStructStepLeafData
	var groups, branches, hashes []uint16

	leafData.Value = rlphacks.RlpEncodedBytes(val1)
	groups, branches, hashes, _ = GenStructStep(retain, keybytesToHex(key1), keybytesToHex(key2), hb, nil, &leafData, groups, branches, hashes, false)

	leafData.Value = rlphacks.RlpEncodedBytes(val2)
	groups, branches, hashes, _ = GenStructStep(retain, keybytesToHex(key2), []byte{}, hb, nil, &leafData, groups, branches, hashes, false)

	root, _ := hb.RootHash()

	testTrie := NewTestRLPTrie(common.Hash{})
	testTrie.Update(key1, val1)
	testTrie.Update(key2, val2)

	legacyRoot := testTrie.Hash()

	assert.Equal(t, root, legacyRoot)
}
