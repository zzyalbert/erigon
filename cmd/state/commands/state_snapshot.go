package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/generate"
	"github.com/spf13/cobra"
)

func init() {
	withChaindata(generateStateSnapshotCmd)
	withSnapshotFile(generateStateSnapshotCmd)
	withSnapshotData(generateStateSnapshotCmd)
	withBlock(generateStateSnapshotCmd)
	rootCmd.AddCommand(generateStateSnapshotCmd)
}

var generateStateSnapshotCmd = &cobra.Command{
	Use:   "stateSnapshot",
	Short: "Generate state snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		return generate.GenerateStateSnapshot(chaindata, snapshotFile, block, snapshotDir, snapshotMode)
	},
}
