package snapshotsync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"os"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

const (
	SnapshotBlock    = 11_000_000
	HeaderHash11kk = "0x4d5e647b2d8d3a3c8c1561ebb88734bc5fc3c2941016f810cf218738c0ecd99e"
	Header11kk = "f90211a01cb6a590440a9ed02e8762ac35faa04ec30cdbcaff0b276fa1ab5e2339033a6aa01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347944bb96091ee9d802ed039c4d1a5f6216f90f81b01a08b2258fc3693f6ed1102f3142839a174b27f215841d2f542b586682898981c6da07d63a1ceded7864e95f09fe65b6bd17fb3f02a3644b1340bb0ab8a7267251e62a04cb5cf79c8a58a4787ec1ed0af51bcce19e6ad701dd40a45086244b933104cf2b901002438522b194b05881a7d976aa8c45ff47193ba8adc6fe2cc85eb68c66503558fa0ba43cebbd2327cfa297a87228511374ed3a2f66f3999426dced224c464840303de108b8604dcafce84d678b589cbe8a74aa2c540668a9a9acfa1eb94c6569918d819063600c000f3c060d649129f8327cad2c7ba1f9495531224b34a1ad8ca0810ab2d2d43a18877484dc33d220c0531024f1dc7448f8a6c016340ae143efd87c5e681d40a34e6be5803ea696038d3ad090048cb267a2ae72e7290da6b385f9874c002302c85e96005aa08031e30ac2a8a9a021bdc2a7a39a1089a08586cefcb937700ff03e4acaa37448c00f4ad02116216437bc52846ebd205869231e574870bf465887ac96883a7d8c083bdfd7483bde3a8845f7befc090505059452d657468706f6f6c2d757331a07a1a8c57afdf3be769e0f6a54e92900374cc207c7cf01b9da6ccca80a8b4006c88d495a5d800490fad"


)
/*
  ,BranchPageN,LeafPageN,OverflowN,Entries
b,4085,326718,0,11635055
eth_tx,212580,47829931,6132023,969755142
 */

var (
	HeaderNumberGenerationStage    = stages.SyncStage("snapshot_header_number")
	HeaderCanonicalGenerationStage = stages.SyncStage("snapshot_canonical")
	Snapshot11kkTD = []byte{138, 3, 199, 118, 5, 203, 95, 162, 81, 64, 161}
)

func PostProcessing(db ethdb.Database, mode SnapshotMode, downloadedSnapshots map[SnapshotType]*SnapshotsInfo) error {
	if mode.State && !mode.Bodies && !mode.Headers {
		return PostProcessNoBlocksSync(db, SnapshotBlock, common.HexToHash(HeaderHash11kk), common.Hex2Bytes(Header11kk))
	}

	if mode.Headers {
		err := GenerateHeaderIndexes(context.Background(), db)
		if err != nil {
			return err
		}
	}

	if mode.Bodies {
		err := PostProcessBodies(db)
		if err != nil {
			return err
		}
	}

	if mode.State {
		err := PostProcessState(db, downloadedSnapshots[SnapshotType_state])
		if err != nil {
			return err
		}
	}
	return nil
}

func PostProcessBodies(db ethdb.Database) error {
	v, err := stages.GetStageProgress(db, stages.Bodies)
	if err != nil {
		return err
	}

	if v > 0 {
		return nil
	}

	k, body, err := db.Last(dbutils.BlockBodyPrefix)
	if err != nil {
		return err
	}

	if body == nil {
		return fmt.Errorf("empty body for key %s", common.Bytes2Hex(k))
	}

	number := binary.BigEndian.Uint64(k[:8])
	err = stages.SaveStageProgress(db, stages.Bodies, number)
	if err != nil {
		return err
	}
	return nil
}

func PostProcessState(db ethdb.GetterPutter, info *SnapshotsInfo) error {
	v, err := stages.GetStageProgress(db, stages.Execution)
	if err != nil {
		return err
	}

	if v > 0 {
		return nil
	}

	err = stages.SaveStageProgress(db, stages.Execution, info.SnapshotBlock)
	if err != nil {
		return err
	}
	return nil
}

func PostProcessNoBlocksSync(db ethdb.GetterPutter, blockNum uint64, blockHash common.Hash, blockHeaderBytes []byte) error {
	v, err := stages.GetStageProgress(db, stages.Execution)
	if err != nil {
		return err
	}

	if v > 0 {
		return nil
	}

	err = db.Put(dbutils.HeaderPrefix,dbutils.HeaderKey(SnapshotBlock, blockHash), blockHeaderBytes)
	if err != nil {
		return err
	}
	err = db.Put(dbutils.HeaderPrefix,dbutils.EncodeBlockNumber(SnapshotBlock), blockHash.Bytes())
	if err != nil {
		return err
	}
	err = db.Put(dbutils.HeaderNumberPrefix, blockHash.Bytes(), dbutils.EncodeBlockNumber(SnapshotBlock) )
	if err != nil {
		return err
	}
	b,err:=rlp.EncodeToBytes(big.NewInt(0).SetBytes(Snapshot11kkTD))
	if err!=nil {
		return err
	}
	err = db.Put(dbutils.HeaderPrefix, dbutils.HeaderTDKey(SnapshotBlock, blockHash), b)
	if err != nil {
		return err
	}


	err = stages.SaveStageProgress(db, stages.Headers, blockNum)
	if err != nil {
		return err
	}
	err = stages.SaveStageProgress(db, stages.Bodies, blockNum)
	if err != nil {
		return err
	}
	err = stages.SaveStageProgress(db, stages.BlockHashes, blockNum)
	if err != nil {
		return err
	}
	err = stages.SaveStageProgress(db, stages.Senders, blockNum)
	if err != nil {
		return err
	}
	err = stages.SaveStageProgress(db, stages.Execution, blockNum)
	if err != nil {
		return err
	}

	return nil
}


func GenerateHeaderIndexes(ctx context.Context, db ethdb.Database) error {
	var hash common.Hash
	var number uint64

	v, err := stages.GetStageProgress(db, HeaderNumberGenerationStage)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return err
	}

	if v == 0 {
		log.Info("Generate headers hash to number index")
		headHashBytes, innerErr := db.Get(dbutils.HeadersSnapshotInfoBucket, []byte(dbutils.SnapshotHeadersHeadHash))
		if innerErr != nil {
			return innerErr
		}

		headNumberBytes, innerErr := db.Get(dbutils.HeadersSnapshotInfoBucket, []byte(dbutils.SnapshotHeadersHeadNumber))
		if innerErr != nil {
			return innerErr
		}

		headNumber := big.NewInt(0).SetBytes(headNumberBytes).Uint64()
		headHash := common.BytesToHash(headHashBytes)

		innerErr = etl.Transform("Torrent post-processing 1", db, dbutils.HeaderPrefix, dbutils.HeaderNumberPrefix, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			if len(k) != 8+common.HashLength {
				return nil
			}
			return next(k, common.CopyBytes(k[8:]), common.CopyBytes(k[:8]))
		}, etl.IdentityLoadFunc, etl.TransformArgs{
			Quit: ctx.Done(),
			OnLoadCommit: func(db ethdb.Putter, key []byte, isDone bool) error {
				if !isDone {
					return nil
				}
				return stages.SaveStageProgress(db, HeaderNumberGenerationStage, 1)
			},
			ExtractEndKey: dbutils.HeaderKey(headNumber, headHash),
		})
		if innerErr != nil {
			return innerErr
		}
	}

	v, err = stages.GetStageProgress(db, HeaderCanonicalGenerationStage)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return err
	}
	if v == 0 {
		h := rawdb.ReadHeaderByNumber(db, 0)
		td := h.Difficulty

		log.Info("Generate TD index & canonical")
		err = etl.Transform("Torrent post-processing 2", db, dbutils.HeaderPrefix, dbutils.HeaderPrefix, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			if len(k) != 8+common.HashLength {
				return nil
			}
			header := &types.Header{}
			innerErr := rlp.DecodeBytes(v, header)
			if innerErr != nil {
				return innerErr
			}
			number = header.Number.Uint64()
			hash = header.Hash()
			td = td.Add(td, header.Difficulty)
			tdBytes, innerErr := rlp.EncodeToBytes(td)
			if innerErr != nil {
				return innerErr
			}

			innerErr = next(k, dbutils.HeaderTDKey(header.Number.Uint64(), header.Hash()), tdBytes)
			if innerErr != nil {
				return innerErr
			}

			//canonical
			return next(k, dbutils.HeaderHashKey(header.Number.Uint64()), header.Hash().Bytes())
		}, etl.IdentityLoadFunc, etl.TransformArgs{
			Quit: ctx.Done(),
			OnLoadCommit: func(db ethdb.Putter, key []byte, isDone bool) error {
				if !isDone {
					return nil
				}

				rawdb.WriteHeadHeaderHash(db, hash)
				rawdb.WriteHeaderNumber(db, hash, number)
				err = stages.SaveStageProgress(db, stages.Headers, number)
				if err != nil {
					return err
				}
				err = stages.SaveStageProgress(db, stages.BlockHashes, number)
				if err != nil {
					return err
				}
				rawdb.WriteHeadBlockHash(db, hash)
				return stages.SaveStageProgress(db, HeaderCanonicalGenerationStage, number)
			},
		})
		if err != nil {
			return err
		}
		log.Info("Last processed block", "num", number, "hash", hash.String())
	}

	return nil
}
