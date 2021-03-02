package commands

import (
	"encoding/json"
	"github.com/anacrolix/torrent/bencode"
	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/turbo-geth/common/math"
	"strconv"
	"testing"
)



func TestName(t *testing.T) {
	v:=ScrapeResponse{
		Files: map[string]*ScrapeData{
			"data.mdb": &ScrapeData{
				2, 5, 6,
			},
		},
	}



	b,err:=bencode.Marshal(v)
	t.Log(err)
	t.Log(b)
	t.Log(string(b))
	vv,err:=json.Marshal(v)
	t.Log(err)
	t.Log(string(vv))

	mp:=map[string]interface{}{}
	err = bencode.Unmarshal([]byte("d5:filesd20:xxxxxxxxxxxxxxxxxxxxd8:completei11e10:downloadedi13772e10:incompletei19e\n20:yyyyyyyyyyyyyyyyyyyyd8:completei21e10:downloadedi206e10:incompletei20eee"), mp)
	t.Log(err)
	spew.Dump(mp)
}
func TestName2(t *testing.T) {
	var v uint64
	t.Log(231928233984)
	t.Log(math.MaxInt64)
	t.Log(v)
	t.Log(v-1)
	t.Log(v-2)
	t.Log(strconv.ParseUint("18446744073709551615", 10, 64))

}