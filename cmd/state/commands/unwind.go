package commands

import (
	"errors"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/torrent"
	"github.com/spf13/cobra"
	"os"
)

func init() {
	rootCmd.AddCommand(remoteSeedSnapshotCmd)
}

var unwindCmd = &cobra.Command{
	Use:   "unwind",
	Short: "unwind state",
	RunE: func(cmd *cobra.Command, args []string) error {
		if snapshotPath == "" {
			return errors.New("empty snapshot path")
		}
		//chaindata, snapshotFile, block, snapshotDir, snapshotMode
		//dbPath, snapshotPath string, toBlock uint64, snapshotDir string, snapshotMode string
		err := os.RemoveAll(snapshotPath)
		if err != nil {
			return err
		}
		kv := ethdb.NewLMDB().Path(chaindata).MustOpen()

		if snapshotDir != "" {
			var mode torrent.SnapshotMode
			mode, err = torrent.SnapshotModeFromString(snapshotMode)
			if err != nil {
				return err
			}

			kv, err = torrent.WrapBySnapshots(kv, snapshotDir, mode)
			if err != nil {
				return err
			}
		}

		return nil
	},
}