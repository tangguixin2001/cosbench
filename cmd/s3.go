package cmd

import "github.com/spf13/cobra"

func buildS3Cmd(parentCmd *cobra.Command) {
	var ()
	var cmd = &cobra.Command{
		Use:   "s3",
		Short: "s3API test",
	}

	parentCmd.AddCommand(cmd)

	buildCOSCheckCmd(cmd)
	buildCOSMultipartUploadCheckCmd(cmd)
	buildCOSRateCmd(cmd)
	buildCOSClearCmd(cmd)
	buildCOSCheck2Cmd(cmd)

	buildVersionsCmd(cmd)
}
