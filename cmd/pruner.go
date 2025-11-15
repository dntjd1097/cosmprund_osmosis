package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/cometbft/cometbft/libs/log"
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

// resourceStats holds resource usage statistics
type resourceStats struct {
	startTime     time.Time
	startRusage   syscall.Rusage
	startMemStats runtime.MemStats
	peakMemory    uint64
}

// newResourceStats creates and initializes resource tracking
func newResourceStats() *resourceStats {
	rs := &resourceStats{
		startTime: time.Now(),
	}

	// Get initial CPU/IO stats
	syscall.Getrusage(syscall.RUSAGE_SELF, &rs.startRusage)

	// Get initial memory stats
	runtime.ReadMemStats(&rs.startMemStats)
	rs.peakMemory = rs.startMemStats.Alloc

	return rs
}

// updatePeakMemory updates peak memory usage
func (rs *resourceStats) updatePeakMemory() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if m.Alloc > rs.peakMemory {
		rs.peakMemory = m.Alloc
	}
}

// printStats prints final resource usage statistics
func (rs *resourceStats) printStats() {
	duration := time.Since(rs.startTime)

	// Get final CPU/IO stats
	var endRusage syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &endRusage)

	// Get final memory stats
	var endMemStats runtime.MemStats
	runtime.ReadMemStats(&endMemStats)

	// Calculate CPU time (user + system)
	userCPU := float64(endRusage.Utime.Sec-rs.startRusage.Utime.Sec) +
		float64(endRusage.Utime.Usec-rs.startRusage.Utime.Usec)/1e6
	sysCPU := float64(endRusage.Stime.Sec-rs.startRusage.Stime.Sec) +
		float64(endRusage.Stime.Usec-rs.startRusage.Stime.Usec)/1e6
	totalCPU := userCPU + sysCPU

	// Calculate disk I/O (blocks)
	blocksIn := endRusage.Inblock - rs.startRusage.Inblock
	blocksOut := endRusage.Oublock - rs.startRusage.Oublock

	fmt.Println("\n=== Resource Usage ===")
	fmt.Printf("Total time: %s\n", duration)
	fmt.Printf("CPU time: %.2fs (user: %.2fs, system: %.2fs)\n", totalCPU, userCPU, sysCPU)
	fmt.Printf("CPU usage: %.1f%%\n", (totalCPU/duration.Seconds())*100)
	fmt.Printf("Peak memory: %.2f MB\n", float64(rs.peakMemory)/(1024*1024))
	fmt.Printf("Final memory: %.2f MB\n", float64(endMemStats.Alloc)/(1024*1024))
	fmt.Printf("Total allocated: %.2f MB\n", float64(endMemStats.TotalAlloc)/(1024*1024))
	fmt.Printf("Disk I/O: %d blocks in, %d blocks out\n", blocksIn, blocksOut)
	fmt.Printf("GC runs: %d\n", endMemStats.NumGC-rs.startMemStats.NumGC)
}

func pruneCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "prune [path_to_home]",
		Short: "prune data from the application store and block store",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Initialize resource tracking
			stats := newResourceStats()
			fmt.Println("Starting pruning...")

			// Start a goroutine to periodically update peak memory
			done := make(chan bool)
			go func() {
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						stats.updatePeakMemory()
					case <-done:
						return
					}
				}
			}()

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

			err := errs.Wait()

			// Stop memory monitoring
			close(done)

			// Print resource usage statistics
			stats.printStats()

			return err
		},
	}
	return cmd
}

func pruneAppState(home string) error {

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
	appDB, err := db.NewGoLevelDBWithOpts("application", dbDir, &o)
	if err != nil {
		return err
	}

	//TODO: need to get all versions in the store, setting randomly is too slow
	fmt.Println("pruning application state")

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

	// TODO: cleanup app state
	appStore := rootmulti.NewStore(appDB, log.NewNopLogger())

	// Disable IAVL fast node to skip expensive upgrade process
	// Fast node is only useful for queries, not needed for pruning
	appStore.SetIAVLDisableFastNode(true)

	for _, value := range keys {
		appStore.MountStoreWithDB(value, storetypes.StoreTypeIAVL, nil)
	}

	err = appStore.LoadLatestVersion()
	if err != nil {
		return err
	}

	latestHeight := rootmulti.GetLatestVersion(appDB)
	// valid heights should be greater than 0.
	if latestHeight <= 0 {
		return fmt.Errorf("the database has no valid heights to prune, the latest height: %v", latestHeight)
	}

	// var pruningHeights []int64
	// for height := int64(1); height < latestHeight; height++ {
	// 	if height < latestHeight-int64(versions) {
	// 		pruningHeights = append(pruningHeights, height)
	// 	}
	// }
	pruneHeight := latestHeight - int64(versions)
	if pruneHeight <= 0 {
		fmt.Println("no heights to prune")
		return nil
	}
	pruningHeights := []int64{pruneHeight}
	//pruningHeight := []int64{latestHeight - int64(versions)}

	if err = appStore.PruneStores(false, pruningHeights); err != nil {
		return err
	}
	fmt.Println("pruning application state complete")

	fmt.Println("compacting application state")
	if err := appDB.Compact(nil, nil); err != nil {
		return err
	}
	fmt.Println("compacting application state complete")

	//create a new app store
	return nil
}

// pruneTMData prunes the tendermint blocks and state based on the amount of blocks to keep
func pruneTMData(home string) error {

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
	blockStoreDB, err := db.NewGoLevelDBWithOpts("blockstore", dbDir, &o)
	if err != nil {
		return err
	}
	blockStore := tmstore.NewBlockStore(blockStoreDB)

	base := blockStore.Base()

	pruneHeight := blockStore.Height() - int64(blocks)

	// Check if there's anything to prune
	if pruneHeight <= base {
		fmt.Printf("No blocks to prune (base: %d, target: %d)\n", base, pruneHeight)
		return nil
	}

	errs, _ := errgroup.WithContext(context.Background())
	errs.Go(func() error {
		fmt.Println("pruning block store")
		// prune block store
		blocks, err = blockStore.PruneBlocks(pruneHeight)
		if err != nil {
			return err
		}
		fmt.Println("pruning block store complete")

		fmt.Println("compacting block store")
		if err := blockStoreDB.Compact(nil, nil); err != nil {
			return err
		}
		fmt.Println("compacting block store complete")

		return nil
	})

	// Get StateStore
	stateDB, err := db.NewGoLevelDBWithOpts("state", dbDir, &o)
	if err != nil {
		return err
	}

	stateStore := state.NewStore(stateDB, state.StoreOptions{
		DiscardABCIResponses: true,
	})
	fmt.Println("pruning state store")
	// prune state store
	err = stateStore.PruneStates(base, pruneHeight)
	if err != nil {
		return err
	}
	fmt.Println("pruning state store complete")

	fmt.Println("compacting state store")
	if err := stateDB.Compact(nil, nil); err != nil {
		return err
	}
	fmt.Println("compacting state store complete")

	return nil
}

// Utils

func rootify(path, root string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(root, path)
}
