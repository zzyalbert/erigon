package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/generate"
	"github.com/spf13/cobra"
)

func init() {
	withChaindata(verifyStateSnapshotCmd)
	withSnapshotFile(verifyStateSnapshotCmd)
	withBlock(verifyStateSnapshotCmd)
	rootCmd.AddCommand(verifyStateSnapshotCmd)
}

var verifyStateSnapshotCmd = &cobra.Command{
	Use:   "verifyStateSnapshot",
	Short: "verify state snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		return generate.VerifyStateSnapshot(chaindata, snapshotFile, block)
	},
}
