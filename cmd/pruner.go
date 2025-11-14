package cmd

import (
	"context"
	"fmt"
	"path/filepath"

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

	// this has the potential to expand size, should just use state sync
	// dbType := db.BackendType(backend)

	dbDir := rootify(dataDir, home)

	o := opt.Options{
		DisableSeeksCompaction: true,
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

	var pruningHeights []int64
	for height := int64(1); height < latestHeight; height++ {
		if height < latestHeight-int64(versions) {
			pruningHeights = append(pruningHeights, height)
		}
	}

	//pruningHeight := []int64{latestHeight - int64(versions)}

	if len(pruningHeights) == 0 {
		fmt.Println("no heights to prune")
		return nil
	}

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

	o := opt.Options{
		DisableSeeksCompaction: true,
	}

	// Get BlockStore
	blockStoreDB, err := db.NewGoLevelDBWithOpts("blockstore", dbDir, &o)
	if err != nil {
		return err
	}
	blockStore := tmstore.NewBlockStore(blockStoreDB)

	// Get StateStore
	stateDB, err := db.NewGoLevelDBWithOpts("state", dbDir, &o)
	if err != nil {
		return err
	}

	stateStore := state.NewStore(stateDB, state.StoreOptions{
		DiscardABCIResponses: true,
	})

	base := blockStore.Base()

	pruneHeight := blockStore.Height() - int64(blocks)

	errs, _ := errgroup.WithContext(context.Background())

	// Block Store pruning and compacting (goroutine 1)
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

	// State Store pruning and compacting (goroutine 2) - independent from blockStore
	errs.Go(func() error {
		fmt.Println("pruning state store")
		// prune state store
		err := stateStore.PruneStates(base, pruneHeight)
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
	})

	// Wait for all goroutines to complete
	return errs.Wait()
}

// Utils

func rootify(path, root string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(root, path)
}
