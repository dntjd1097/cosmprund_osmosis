package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/cosmos/cosmos-sdk/types"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	authzkeeper "github.com/cosmos/cosmos-sdk/x/authz/keeper"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	capabilitytypes "github.com/cosmos/cosmos-sdk/x/capability/types"
	packetforwardtypes "github.com/cosmos/ibc-apps/middleware/packet-forward-middleware/v7/packetforward/types"
	icqtypes "github.com/cosmos/ibc-apps/modules/async-icq/v7/types"
	icahosttypes "github.com/cosmos/ibc-go/v7/modules/apps/27-interchain-accounts/host/types"
	ibctransfertypes "github.com/cosmos/ibc-go/v7/modules/apps/transfer/types"
	ibchost "github.com/cosmos/ibc-go/v7/modules/core/exported"

	db "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/state"
	tmstore "github.com/cometbft/cometbft/store"
	storetypes "github.com/cosmos/cosmos-sdk/store/types"
	consensusparamtypes "github.com/cosmos/cosmos-sdk/x/consensus/types"
	crisistypes "github.com/cosmos/cosmos-sdk/x/crisis/types"
	distrtypes "github.com/cosmos/cosmos-sdk/x/distribution/types"
	evidencetypes "github.com/cosmos/cosmos-sdk/x/evidence/types"
	govtypes "github.com/cosmos/cosmos-sdk/x/gov/types"
	minttypes "github.com/cosmos/cosmos-sdk/x/mint/types"
	paramstypes "github.com/cosmos/cosmos-sdk/x/params/types"
	slashingtypes "github.com/cosmos/cosmos-sdk/x/slashing/types"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	upgradetypes "github.com/cosmos/cosmos-sdk/x/upgrade/types"
	"github.com/neilotoole/errgroup"
	"github.com/spf13/cobra"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/binaryholdings/cosmos-pruner/internal/rootmulti"
)

// load db
// load app store and prune
// if immutable tree is not deletable we should import and export current state

func pruneCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "prune [path_to_home]",
		Short: "prune data from the application store and block store",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {

			ctx := cmd.Context()
			errs, _ := errgroup.WithContext(ctx)

			// Tendermint pruning (blockstore.db, state.db)
			if tendermint {
				errs.Go(func() error {
					return pruneTMData(args[0])
				})
			}

			// CosmosSDK pruning (application.db)
			if cosmosSdk {
				errs.Go(func() error {
					return pruneAppState(args[0])
				})
			}

			return errs.Wait()
		},
	}
	return cmd
}

func pruneAppState(home string) error {
	logger.Info("=== Starting Application State Pruning ===")
	totalStartTime := time.Now()

	// this has the potential to expand size, should just use state sync
	// dbType := db.BackendType(backend)

	dbDir := rootify(dataDir, home)

	// Optimize LevelDB options for faster compaction while maintaining compression
	o := opt.Options{
		DisableSeeksCompaction: true,

		// === Speed optimizations ===
		// Increase write buffer to reduce compaction frequency (default 4MB -> 128MB)
		WriteBuffer: 128 * 1024 * 1024,

		// Increase block size for better I/O efficiency (default 4KB -> 32KB)
		BlockSize: 32 * 1024,

		// Increase compaction table size to reduce compaction frequency (default 2MB -> 16MB)
		CompactionTableSize: 16 * 1024 * 1024,

		// Increase total size multiplier to reduce number of levels (default 10)
		CompactionTotalSizeMultiplier: 8.0,

		// Increase open file limit for better performance
		OpenFilesCacheCapacity: 2000,

		// Block cache for faster reads
		BlockCacheCapacity: 64 * 1024 * 1024, // 64MB cache

		// === Compression (Snappy - fast and efficient) ===
		Compression: opt.SnappyCompression,
	}

	// Override compression if user explicitly disabled it
	if noCompression {
		o.Compression = opt.NoCompression
	}

	// Get BlockStore
	logger.Info("[1/6] Opening application database...")
	stepStart := time.Now()
	appDB, err := db.NewGoLevelDBWithOpts("application", dbDir, &o)
	if err != nil {
		return err
	}
	logger.Info("[1/6] Database opened", "duration", time.Since(stepStart).String())

	compressionType := "snappy"
	if noCompression {
		compressionType = "disabled"
	}
	logger.Info("application database options configured",
		"writeBuffer", "128MB",
		"blockSize", "32KB",
		"compactionTableSize", "16MB",
		"blockCache", "64MB",
		"compression", compressionType,
		"openFiles", 2000)

	//TODO: need to get all versions in the store, setting randomly is too slow
	logger.Info("[2/6] Preparing store keys...")
	stepStart = time.Now()

	// only mount keys from core sdk
	// todo allow for other keys to be mounted
	keys := types.NewKVStoreKeys(
		authtypes.StoreKey, banktypes.StoreKey, authzkeeper.StoreKey, stakingtypes.StoreKey, distrtypes.StoreKey, slashingtypes.StoreKey, ibchost.StoreKey,
		icahosttypes.StoreKey,
		icqtypes.StoreKey,
		evidencetypes.StoreKey, minttypes.StoreKey, govtypes.StoreKey, ibctransfertypes.StoreKey,
		packetforwardtypes.StoreKey,
		paramstypes.StoreKey, consensusparamtypes.StoreKey, capabilitytypes.StoreKey, crisistypes.StoreKey, upgradetypes.StoreKey,
		// feegrant.StoreKey,
	)

	if app == "osmosis" {
		osmoKeys := types.NewKVStoreKeys(
			"downtimedetector",
			"hooks-for-ibc",
			"lockup", //lockuptypes.StoreKey,
			"concentratedliquidity",
			"gamm", // gammtypes.StoreKey,
			"cosmwasmpool",
			"poolmanager",
			"twap",
			"epochs", // epochstypes.StoreKey,
			"protorev",
			"txfees",         // txfeestypes.StoreKey,
			"incentives",     // incentivestypes.StoreKey,
			"poolincentives", //poolincentivestypes.StoreKey,
			"tokenfactory",   //tokenfactorytypes.StoreKey,
			"valsetpref",
			"superfluid", // superfluidtypes.StoreKey,
			"wasm",       // wasm.StoreKey,
			//"rate-limited-ibc", // there is no store registered for this module
		)
		for key, value := range osmoKeys {
			keys[key] = value
		}
	}

	logger.Info("[2/6] Store keys prepared", "totalStores", len(keys), "duration", time.Since(stepStart).String())

	// TODO: cleanup app state
	logger.Info("[3/6] Creating and mounting stores...")
	stepStart = time.Now()
	appStore := rootmulti.NewStore(appDB, logger)

	for _, value := range keys {
		appStore.MountStoreWithDB(value, storetypes.StoreTypeIAVL, nil)
	}
	logger.Info("[3/6] Stores mounted", "duration", time.Since(stepStart).String())

	logger.Info("[4/6] Loading latest version (reading IAVL trees from disk)...")
	stepStart = time.Now()
	err = appStore.LoadLatestVersion()
	if err != nil {
		return err
	}
	logger.Info("[4/6] Latest version loaded", "duration", time.Since(stepStart).String())

	latestHeight := rootmulti.GetLatestVersion(appDB)
	// valid heights should be greater than 0.
	if latestHeight <= 0 {
		return fmt.Errorf("the database has no valid heights to prune, the latest height: %v", latestHeight)
	}

	logger.Info("[5/6] Calculating pruning heights...")
	stepStart = time.Now()
	var pruningHeights []int64
	for height := int64(1); height < latestHeight; height++ {
		if height < latestHeight-int64(versions) {
			pruningHeights = append(pruningHeights, height)
		}
	}

	//pruningHeight := []int64{latestHeight - int64(versions)}

	if len(pruningHeights) == 0 {
		logger.Info("no heights to prune")
		totalDuration := time.Since(totalStartTime)
		logger.Info("=== Application State Pruning Complete (No Action) ===", "totalDuration", totalDuration.String())
		return nil
	}

	pruneUpToHeight := pruningHeights[len(pruningHeights)-1]
	logger.Info("[5/6] Pruning heights calculated",
		"latestHeight", latestHeight,
		"keepVersions", versions,
		"pruneUpToHeight", pruneUpToHeight,
		"totalHeightsToPrune", len(pruningHeights),
		"duration", time.Since(stepStart).String())

	logger.Info("[5/6] Pruning stores (deleting old IAVL versions)...")
	logger.Info("⚠️  This step may take a long time depending on data size")
	pruneStartTime := time.Now()
	if err = appStore.PruneStores(false, pruningHeights); err != nil {
		return err
	}
	pruneDuration := time.Since(pruneStartTime)
	logger.Info("[5/6] Pruning stores complete", "duration", pruneDuration.String())

	logger.Info("[6/6] Compacting database (reclaiming disk space)...")
	logger.Info("⚠️  This step may also take a long time")
	compactStartTime := time.Now()
	if err := appDB.Compact(nil, nil); err != nil {
		return err
	}
	compactDuration := time.Since(compactStartTime)
	logger.Info("[6/6] Database compaction complete", "duration", compactDuration.String())

	totalDuration := time.Since(totalStartTime)
	logger.Info("=== Application State Pruning Complete ===",
		"totalDuration", totalDuration.String(),
		"loadTime", stepStart.Sub(totalStartTime).String(),
		"pruneTime", pruneDuration.String(),
		"compactTime", compactDuration.String())

	//create a new app store
	return nil
}

// pruneTMData prunes the tendermint blocks and state based on the amount of blocks to keep
func pruneTMData(home string) error {
	logger.Info("=== Starting Tendermint Data Pruning ===")
	totalStartTime := time.Now()

	dbDir := rootify(dataDir, home)

	// Optimize LevelDB options for faster compaction while maintaining compression
	o := opt.Options{
		DisableSeeksCompaction: true,

		// === Speed optimizations ===
		// Increase write buffer to reduce compaction frequency (default 4MB -> 128MB)
		WriteBuffer: 128 * 1024 * 1024,

		// Increase block size for better I/O efficiency (default 4KB -> 32KB)
		BlockSize: 32 * 1024,

		// Increase compaction table size to reduce compaction frequency (default 2MB -> 16MB)
		CompactionTableSize: 16 * 1024 * 1024,

		// Increase total size multiplier to reduce number of levels (default 10)
		CompactionTotalSizeMultiplier: 8.0,

		// Increase open file limit for better performance
		OpenFilesCacheCapacity: 2000,

		// Block cache for faster reads
		BlockCacheCapacity: 64 * 1024 * 1024, // 64MB cache

		// === Compression (Snappy - fast and efficient) ===
		Compression: opt.SnappyCompression,
	}

	// Override compression if user explicitly disabled it
	if noCompression {
		o.Compression = opt.NoCompression
	}

	// Get BlockStore
	logger.Info("[TM 1/4] Opening blockstore database...")
	stepStart := time.Now()
	blockStoreDB, err := db.NewGoLevelDBWithOpts("blockstore", dbDir, &o)
	if err != nil {
		return err
	}
	blockStore := tmstore.NewBlockStore(blockStoreDB)
	logger.Info("[TM 1/4] Blockstore opened", "duration", time.Since(stepStart).String())

	// Get StateStore
	logger.Info("[TM 2/4] Opening state database...")
	stepStart = time.Now()
	stateDB, err := db.NewGoLevelDBWithOpts("state", dbDir, &o)
	if err != nil {
		return err
	}
	logger.Info("[TM 2/4] State database opened", "duration", time.Since(stepStart).String())

	compressionType := "snappy"
	if noCompression {
		compressionType = "disabled"
	}
	logger.Info("tendermint database options configured",
		"writeBuffer", "128MB",
		"blockSize", "32KB",
		"compactionTableSize", "16MB",
		"blockCache", "64MB",
		"compression", compressionType,
		"openFiles", 2000)

	stateStore := state.NewStore(stateDB, state.StoreOptions{
		DiscardABCIResponses: true,
	})

	base := blockStore.Base()
	currentHeight := blockStore.Height()
	pruneHeight := currentHeight - int64(blocks)

	logger.Info("[TM 3/4] Calculated pruning range",
		"currentHeight", currentHeight,
		"keepBlocks", blocks,
		"pruneUpToHeight", pruneHeight,
		"base", base)

	logger.Info("[TM 4/4] Pruning and compacting (parallel execution)...")
	parallelStartTime := time.Now()

	errs, _ := errgroup.WithContext(context.Background())

	// Block Store pruning and compacting (goroutine 1)
	errs.Go(func() error {
		logger.Info("  [BlockStore] Starting pruning...")
		pruneStartTime := time.Now()
		// prune block store
		blocks, err = blockStore.PruneBlocks(pruneHeight)
		if err != nil {
			return err
		}
		pruneDuration := time.Since(pruneStartTime)
		logger.Info("  [BlockStore] Pruning complete", "duration", pruneDuration.String())

		logger.Info("  [BlockStore] Starting compaction...")
		compactStartTime := time.Now()
		if err := blockStoreDB.Compact(nil, nil); err != nil {
			return err
		}
		compactDuration := time.Since(compactStartTime)
		logger.Info("  [BlockStore] Compaction complete", "duration", compactDuration.String())

		totalDuration := time.Since(pruneStartTime)
		logger.Info("  [BlockStore] Total time", "duration", totalDuration.String())

		return nil
	})

	// State Store pruning and compacting (goroutine 2) - independent from blockStore
	errs.Go(func() error {
		logger.Info("  [StateStore] Starting pruning...")
		pruneStartTime := time.Now()
		// prune state store
		err := stateStore.PruneStates(base, pruneHeight)
		if err != nil {
			return err
		}
		pruneDuration := time.Since(pruneStartTime)
		logger.Info("  [StateStore] Pruning complete", "duration", pruneDuration.String())

		logger.Info("  [StateStore] Starting compaction...")
		compactStartTime := time.Now()
		if err := stateDB.Compact(nil, nil); err != nil {
			return err
		}
		compactDuration := time.Since(compactStartTime)
		logger.Info("  [StateStore] Compaction complete", "duration", compactDuration.String())

		totalDuration := time.Since(pruneStartTime)
		logger.Info("  [StateStore] Total time", "duration", totalDuration.String())

		return nil
	})

	// Wait for all goroutines to complete
	err = errs.Wait()
	if err != nil {
		return err
	}

	parallelDuration := time.Since(parallelStartTime)
	totalDuration := time.Since(totalStartTime)
	logger.Info("[TM 4/4] Parallel operations complete", "duration", parallelDuration.String())
	logger.Info("=== Tendermint Data Pruning Complete ===", "totalDuration", totalDuration.String())

	return nil
}

// Utils

func rootify(path, root string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(root, path)
}
