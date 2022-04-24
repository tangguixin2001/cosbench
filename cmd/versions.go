package cmd

import "github.com/spf13/cobra"

func buildVersionsCmd(parentCmd *cobra.Command) {
	var ()
	var cmd = &cobra.Command{
		Use:   "versions",
		Short: "versions bucket test",
	}

	parentCmd.AddCommand(cmd)
}
