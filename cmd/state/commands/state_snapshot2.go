package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/generate"
	"github.com/spf13/cobra"
)

func init() {
	withChaindata(generateStateSnapshotCmd2)
	withSnapshotFile(generateStateSnapshotCmd2)
	withSnapshotData(generateStateSnapshotCmd2)
	withBlock(generateStateSnapshotCmd2)
	rootCmd.AddCommand(generateStateSnapshotCmd2)
}

var generateStateSnapshotCmd2 = &cobra.Command{
	Use:   "stateSnapshot2",
	Short: "Generate state snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		return generate.GenerateStateSnapshot2(chaindata, snapshotFile, block, snapshotDir, snapshotMode)
	},
}
