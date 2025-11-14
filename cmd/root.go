package cmd

import (
	"os"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	dataDir       string
	backend       string
	app           string
	cosmosSdk     bool
	tendermint    bool
	blocks        uint64
	versions      uint64
	verbose       bool
	noCompression bool
	appName       = "cosmprund"
	logger        log.Logger
)

// NewRootCmd returns the root command for relayer.
func NewRootCmd() *cobra.Command {
	// RootCmd represents the base command when called without any subcommands
	var rootCmd = &cobra.Command{
		Use:   appName,
		Short: "cosmprund is meant to prune data base history from a cosmos application, avoiding needing to state sync every couple amount of weeks",
	}

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		// Initialize logger with timestamp and caller info
		logger = log.NewTMLogger(log.NewSyncWriter(os.Stdout))

		// Set log level based on verbose flag
		if verbose {
			// Show all logs including Debug level
			logger = log.NewFilter(logger, log.AllowDebug())
		} else {
			// Only show Info level and above (hides Debug logs like loadVersion commitID)
			logger = log.NewFilter(logger, log.AllowInfo())
		}

		return nil
	}

	// --blocks flag
	rootCmd.PersistentFlags().Uint64VarP(&blocks, "blocks", "b", 10, "set the amount of blocks to keep (default=10)")
	if err := viper.BindPFlag("blocks", rootCmd.PersistentFlags().Lookup("blocks")); err != nil {
		panic(err)
	}

	// --versions flag
	rootCmd.PersistentFlags().Uint64VarP(&versions, "versions", "v", 10, "set the amount of versions to keep in the application store (default=10)")
	if err := viper.BindPFlag("versions", rootCmd.PersistentFlags().Lookup("versions")); err != nil {
		panic(err)
	}

	// --backend flag
	rootCmd.PersistentFlags().StringVar(&backend, "backend", "goleveldb", "set the type of db being used(default=goleveldb)") //todo add list of dbs to comment
	if err := viper.BindPFlag("backend", rootCmd.PersistentFlags().Lookup("backend")); err != nil {
		panic(err)
	}

	// --app flag
	rootCmd.PersistentFlags().StringVar(&app, "app", "osmosis", "set the app you are pruning (supported apps: osmosis)")
	if err := viper.BindPFlag("app", rootCmd.PersistentFlags().Lookup("app")); err != nil {
		panic(err)
	}

	// --cosmos-sdk flag
	rootCmd.PersistentFlags().BoolVar(&cosmosSdk, "cosmos-sdk", true, "set to false if using only with tendermint (default true)")
	if err := viper.BindPFlag("cosmos-sdk", rootCmd.PersistentFlags().Lookup("cosmos-sdk")); err != nil {
		panic(err)
	}

	// --tendermint flag
	rootCmd.PersistentFlags().BoolVar(&tendermint, "tendermint", true, "set to false you dont want to prune tendermint data(default true)")
	if err := viper.BindPFlag("tendermint", rootCmd.PersistentFlags().Lookup("tendermint")); err != nil {
		panic(err)
	}

	// --verbose flag
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "enable verbose logging (shows debug logs)")
	if err := viper.BindPFlag("verbose", rootCmd.PersistentFlags().Lookup("verbose")); err != nil {
		panic(err)
	}

	// --no-compression flag
	rootCmd.PersistentFlags().BoolVar(&noCompression, "no-compression", false, "disable compression (default false, compression enabled)")
	if err := viper.BindPFlag("no-compression", rootCmd.PersistentFlags().Lookup("no-compression")); err != nil {
		panic(err)
	}

	rootCmd.AddCommand(
		pruneCmd(),
	)

	return rootCmd
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	cobra.EnableCommandSorting = false

	rootCmd := NewRootCmd()
	rootCmd.SilenceUsage = true
	rootCmd.CompletionOptions.DisableDefaultCmd = true

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
