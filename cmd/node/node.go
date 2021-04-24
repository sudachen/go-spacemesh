// Package node contains the main executable for go-spacemesh node
package node

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof" // import for memory and network profiling
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"time"

	"cloud.google.com/go/profiler"
	"github.com/spacemeshos/amcl"
	"github.com/spacemeshos/amcl/BLS381"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/spacemeshos/go-spacemesh/activation"
	"github.com/spacemeshos/go-spacemesh/api"
	apiCfg "github.com/spacemeshos/go-spacemesh/api/config"
	"github.com/spacemeshos/go-spacemesh/api/grpcserver"
	"github.com/spacemeshos/go-spacemesh/blocks"
	cmdp "github.com/spacemeshos/go-spacemesh/cmd"
	"github.com/spacemeshos/go-spacemesh/common/types"
	"github.com/spacemeshos/go-spacemesh/common/util"
	cfg "github.com/spacemeshos/go-spacemesh/config"
	"github.com/spacemeshos/go-spacemesh/database"
	"github.com/spacemeshos/go-spacemesh/events"
	"github.com/spacemeshos/go-spacemesh/filesystem"
	"github.com/spacemeshos/go-spacemesh/hare"
	"github.com/spacemeshos/go-spacemesh/hare/eligibility"
	"github.com/spacemeshos/go-spacemesh/log"
	"github.com/spacemeshos/go-spacemesh/mesh"
	"github.com/spacemeshos/go-spacemesh/metrics"
	"github.com/spacemeshos/go-spacemesh/miner"
	"github.com/spacemeshos/go-spacemesh/p2p"
	"github.com/spacemeshos/go-spacemesh/p2p/service"
	"github.com/spacemeshos/go-spacemesh/pendingtxs"
	"github.com/spacemeshos/go-spacemesh/priorityq"
	"github.com/spacemeshos/go-spacemesh/signing"
	"github.com/spacemeshos/go-spacemesh/state"
	"github.com/spacemeshos/go-spacemesh/sync"
	"github.com/spacemeshos/go-spacemesh/timesync"
	timeCfg "github.com/spacemeshos/go-spacemesh/timesync/config"
	"github.com/spacemeshos/go-spacemesh/tortoise"
	"github.com/spacemeshos/go-spacemesh/turbohare"
)

const edKeyFileName = "key.bin"

// Logger names
const (
	AppLogger            = "app"
	P2PLogger            = "p2p"
	PostLogger           = "post"
	StateDbLogger        = "stateDbStore"
	StateLogger          = "state"
	AtxDbStoreLogger     = "atxDbStore"
	PoetDbStoreLogger    = "poetDbStore"
	StoreLogger          = "store"
	PoetDbLogger         = "poetDb"
	MeshDBLogger         = "meshDb"
	TrtlLogger           = "trtl"
	AtxDbLogger          = "atxDb"
	BlkEligibilityLogger = "blkElgValidator"
	MeshLogger           = "mesh"
	SyncLogger           = "sync"
	BlockOracle          = "blockOracle"
	HareBeaconLogger     = "hareBeacon"
	HareOracleLogger     = "hareOracle"
	HareLogger           = "hare"
	BlockBuilderLogger   = "blockBuilder"
	BlockListenerLogger  = "blockListener"
	PoetListenerLogger   = "poetListener"
	NipostBuilderLogger  = "nipostBuilder"
	AtxBuilderLogger     = "atxBuilder"
	GossipListener       = "gossipListener"
)

// Cmd is the cobra wrapper for the node, that allows adding parameters to it
var Cmd = &cobra.Command{
	Use:   "node",
	Short: "start node",
	Run: func(cmd *cobra.Command, args []string) {
		app := NewSpacemeshApp()
		defer app.Cleanup(cmd, args)

		err := app.Initialize(cmd, args)
		if err != nil {
			log.With().Error("Failed to initialize node.", log.Err(err))
			return
		}
		// This blocks until the context is finished
		app.Start(cmd, args)
	},
}

// VersionCmd returns the current version of spacemesh
var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show version info",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Print(cmdp.Version)
		if cmdp.Commit != "" {
			fmt.Printf("+%s", cmdp.Commit)
		}
		fmt.Println()
	},
}

func init() {
	// TODO add commands actually adds flags
	cmdp.AddCommands(Cmd)
	Cmd.AddCommand(VersionCmd)
}

// Service is a general service interface that specifies the basic start/stop functionality
type Service interface {
	Start() error
	Close()
}

// HareService is basic definition of hare algorithm service, providing consensus results for a layer
type HareService interface {
	Service
	GetResult(id types.LayerID) ([]types.BlockID, error)
}

// TickProvider is an interface to a glopbal system clock that releases ticks on each layer
type TickProvider interface {
	Subscribe() timesync.LayerTimer
	Unsubscribe(timer timesync.LayerTimer)
	GetCurrentLayer() types.LayerID
	StartNotifying()
	GetGenesisTime() time.Time
	LayerToTime(id types.LayerID) time.Time
	Close()
	AwaitLayer(layerID types.LayerID) chan struct{}
}

// SpacemeshApp is the cli app singleton
type SpacemeshApp struct {
	*cobra.Command
	nodeID         types.NodeID
	P2P            p2p.Service
	Config         *cfg.Config
	grpcAPIService *grpcserver.Server
	jsonAPIService *grpcserver.JSONHTTPServer
	gatewaySvc     *grpcserver.GatewayService
	globalstateSvc *grpcserver.GlobalStateService
	txService      *grpcserver.TransactionService
	syncer         *sync.Syncer
	blockListener  *blocks.BlockHandler
	state          *state.TransactionProcessor
	blockProducer  *miner.BlockBuilder
	oracle         *blocks.Oracle
	txProcessor    *state.TransactionProcessor
	mesh           *mesh.Mesh
	gossipListener *service.Listener
	clock          TickProvider
	hare           HareService
	postMgr        *activation.PostManager
	atxBuilder     *activation.Builder
	atxDb          *activation.DB
	poetListener   *activation.PoetListener
	edSgn          *signing.EdSigner
	closers        []interface{ Close() }
	log            log.Log
	txPool         *state.TxMempool
	loggers        map[string]*zap.AtomicLevel
	term           chan struct{} // this channel is closed when closing services, goroutines should wait on this channel in order to terminate
	started        chan struct{} // this channel is closed once the app has finished starting
}

// LoadConfigFromFile tries to load configuration file if the config parameter was specified
func LoadConfigFromFile() (*cfg.Config, error) {

	fileLocation := viper.GetString("config")
	vip := viper.New()
	// read in default config if passed as param using viper
	if err := cfg.LoadConfig(fileLocation, vip); err != nil {
		log.Error(fmt.Sprintf("couldn't load config file at location: %s switching to defaults \n error: %v.",
			fileLocation, err))
		// return err
	}

	conf := cfg.DefaultConfig()
	// load config if it was loaded to our viper
	err := vip.Unmarshal(&conf)
	if err != nil {
		log.Error("Failed to parse config\n")
		return nil, err
	}
	return &conf, nil
}

// ParseConfig unmarshal config file into struct
func (app *SpacemeshApp) ParseConfig() error {

	conf, err := LoadConfigFromFile()
	app.Config = conf

	return err
}

// NewSpacemeshApp creates an instance of the spacemesh app
func NewSpacemeshApp() *SpacemeshApp {

	defaultConfig := cfg.DefaultConfig()
	node := &SpacemeshApp{
		Config:  &defaultConfig,
		loggers: make(map[string]*zap.AtomicLevel),
		term:    make(chan struct{}),
		started: make(chan struct{}),
	}

	return node
}

func (app *SpacemeshApp) introduction() {
	log.Info("Welcome to Spacemesh. Spacemesh full node is starting...")
}

// Initialize does pre processing of flags and configuration files, it also initializes data dirs if they dont exist
func (app *SpacemeshApp) Initialize(cmd *cobra.Command, args []string) (err error) {

	// exit gracefully - e.g. with app Cleanup on sig abort (ctrl-c)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	// Goroutine that listens for Ctrl ^ C command
	// and triggers the quit app
	go func() {
		for range signalChan {
			log.Info("Received an interrupt, stopping services...\n")
			cmdp.Cancel()
		}
	}()

	// parse the config file based on flags et al
	err = app.ParseConfig()

	if err != nil {
		log.Error(fmt.Sprintf("couldn't parse the config err=%v", err))
	}

	// ensure cli flags are higher priority than config file
	if err := cmdp.EnsureCLIFlags(cmd, app.Config); err != nil {
		return err
	}

	if app.Config.Profiler {
		if err := profiler.Start(profiler.Config{
			Service:        "go-spacemesh",
			ServiceVersion: fmt.Sprintf("%s+%s+%s", cmdp.Version, cmdp.Branch, cmdp.Commit),
			MutexProfiling: true,
		}); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "failed to start profiler:", err)
		}
	}

	// override default config in timesync since timesync is using TimeCongigValues
	timeCfg.TimeConfigValues = app.Config.TIME

	// ensure all data folders exist
	err = filesystem.ExistOrCreate(app.Config.DataDir())
	if err != nil {
		return err
	}

	app.setupLogging()

	app.introduction()

	drift, err := timesync.CheckSystemClockDrift()
	if err != nil {
		return err
	}

	log.Info("System clock synchronized with ntp. drift: %s", drift)
	return nil
}

// setupLogging configured the app logging system.
func (app *SpacemeshApp) setupLogging() {
	if app.Config.TestMode {
		log.JSONLog(true)
	}

	// app-level logging
	log.InitSpacemeshLoggingSystemWithHooks(func(entry zapcore.Entry) error {
		// If we report anything less than this we'll end up in an infinite loop
		if entry.Level >= zapcore.ErrorLevel {
			events.ReportError(events.NodeError{
				Msg:   entry.Message,
				Trace: string(debug.Stack()),
				Level: entry.Level,
			})
		}
		return nil
	})

	log.Info("%s", app.getAppInfo())

	msg := "initializing event reporter"
	if app.Config.PublishEventsURL != "" {
		msg += fmt.Sprintf(" with pubsub URL: %s", app.Config.PublishEventsURL)
	}
	log.Info(msg)
	if err := events.InitializeEventReporter(app.Config.PublishEventsURL); err != nil {
		log.With().Error("unable to initialize event reporter", log.Err(err))
	}
}

func (app *SpacemeshApp) getAppInfo() string {
	return fmt.Sprintf("App version: %s. Git: %s - %s . Go Version: %s. OS: %s-%s ",
		cmdp.Version, cmdp.Branch, cmdp.Commit, runtime.Version(), runtime.GOOS, runtime.GOARCH)
}

// Cleanup stops all app services
func (app *SpacemeshApp) Cleanup(*cobra.Command, []string) {
	log.Info("app cleanup starting...")
	app.stopServices()
	// add any other Cleanup tasks here....
	log.Info("app cleanup completed\n\n")
}

func (app *SpacemeshApp) setupGenesis(state *state.TransactionProcessor, msh *mesh.Mesh) {
	var conf *apiCfg.GenesisConfig
	if app.Config.GenesisConfPath != "" {
		var err error
		conf, err = apiCfg.LoadGenesisConfig(app.Config.GenesisConfPath)
		if err != nil {
			app.log.Error("cannot load genesis config from file")
		}
	} else {
		conf = apiCfg.DefaultGenesisConfig()
	}
	for id, acc := range conf.InitialAccounts {
		bytes := util.FromHex(id)
		if len(bytes) == 0 {
			// todo: should we panic here?
			app.log.With().Error("cannot read config entry for genesis account", log.String("acct_id", id))
			continue
		}

		addr := types.BytesToAddress(bytes)
		state.CreateAccount(addr)
		state.AddBalance(addr, acc.Balance)
		state.SetNonce(addr, acc.Nonce)
		app.log.With().Info("genesis account created",
			log.String("acct_id", id),
			log.Uint64("balance", acc.Balance))
	}

	_, err := state.Commit()
	if err != nil {
		log.Panic("cannot commit genesis state")
	}
}

type weakCoinStub struct {
}

// GetResult returns the weak coin toss result
func (weakCoinStub) GetResult() bool {
	return true
}

// Wrap the top-level logger to add context info and set the level for a
// specific module.
func (app *SpacemeshApp) addLogger(name string, logger log.Log) log.Log {
	lvl := zap.NewAtomicLevel()
	var err error

	switch name {
	case AppLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.AppLoggerLevel))
	case P2PLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.P2PLoggerLevel))
	case PostLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.PostLoggerLevel))
	case StateDbLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.StateDbLoggerLevel))
	case StateLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.StateLoggerLevel))
	case AtxDbStoreLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.AtxDbStoreLoggerLevel))
	case PoetDbStoreLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.PoetDbStoreLoggerLevel))
	case StoreLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.StoreLoggerLevel))
	case PoetDbLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.PoetDbLoggerLevel))
	case MeshDBLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.MeshDBLoggerLevel))
	case TrtlLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.TrtlLoggerLevel))
	case AtxDbLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.AtxDbLoggerLevel))
	case BlkEligibilityLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.BlkEligibilityLoggerLevel))
	case MeshLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.MeshLoggerLevel))
	case SyncLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.SyncLoggerLevel))
	case BlockOracle:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.BlockOracleLevel))
	case HareOracleLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.HareOracleLoggerLevel))
	case HareBeaconLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.HareBeaconLoggerLevel))
	case HareLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.HareLoggerLevel))
	case BlockBuilderLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.BlockBuilderLoggerLevel))
	case BlockListenerLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.BlockListenerLoggerLevel))
	case PoetListenerLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.PoetListenerLoggerLevel))
	case NipostBuilderLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.NipostBuilderLoggerLevel))
	case AtxBuilderLogger:
		err = lvl.UnmarshalText([]byte(app.Config.LOGGING.AtxBuilderLoggerLevel))
	default:
		lvl.SetLevel(log.Level())
	}

	if err != nil {
		log.Error("cannot parse logging for %v error %v", name, err)
		lvl.SetLevel(log.Level())
	}

	app.loggers[name] = &lvl
	return logger.SetLevel(&lvl).WithName(name)
}

// SetLogLevel updates the log level of an existing logger
func (app *SpacemeshApp) SetLogLevel(name, loglevel string) error {
	if lvl, ok := app.loggers[name]; ok {
		err := lvl.UnmarshalText([]byte(loglevel))
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("cannot find logger %v", name)
	}
	return nil
}

func (app *SpacemeshApp) initServices(nodeID types.NodeID,
	swarm service.Service,
	dbStorepath string,
	sgn hare.Signer,
	isFixedOracle bool,
	rolacle hare.Rolacle,
	layerSize uint32,
	poetClient activation.PoetProvingServiceClient,
	vrfSigner *BLS381.BlsSigner,
	layersPerEpoch uint16, clock TickProvider) error {

	app.nodeID = nodeID

	name := nodeID.ShortString()

	// This base logger must be debug level so that other, derived loggers are not a lower level.
	lg := log.NewWithLevel(name, zap.NewAtomicLevelAt(zapcore.DebugLevel)).WithFields(nodeID)

	types.SetLayersPerEpoch(int32(app.Config.LayersPerEpoch))

	app.log = app.addLogger(AppLogger, lg)

	db, err := database.NewLDBDatabase(filepath.Join(dbStorepath, "state"), 0, 0, app.addLogger(StateDbLogger, lg))
	if err != nil {
		return err
	}
	app.closers = append(app.closers, db)

	coinToss := weakCoinStub{}

	atxdbstore, err := database.NewLDBDatabase(filepath.Join(dbStorepath, "atx"), 0, 0, app.addLogger(AtxDbStoreLogger, lg))
	if err != nil {
		return err
	}
	app.closers = append(app.closers, atxdbstore)

	poetDbStore, err := database.NewLDBDatabase(filepath.Join(dbStorepath, "poet"), 0, 0, app.addLogger(PoetDbStoreLogger, lg))
	if err != nil {
		return err
	}
	app.closers = append(app.closers, poetDbStore)

	iddbstore, err := database.NewLDBDatabase(filepath.Join(dbStorepath, "ids"), 0, 0, app.addLogger(StateDbLogger, lg))
	if err != nil {
		return err
	}
	app.closers = append(app.closers, iddbstore)

	store, err := database.NewLDBDatabase(filepath.Join(dbStorepath, "store"), 0, 0, app.addLogger(StoreLogger, lg))
	if err != nil {
		return err
	}
	app.closers = append(app.closers, store)

	idStore := activation.NewIdentityStore(iddbstore)
	poetDb := activation.NewPoetDb(poetDbStore, app.addLogger(PoetDbLogger, lg))
	validator := activation.NewValidator(poetDb, app.Config.POST)
	mdb, err := mesh.NewPersistentMeshDB(filepath.Join(dbStorepath, "mesh"), app.Config.BlockCacheSize, app.addLogger(MeshDBLogger, lg))
	if err != nil {
		return err
	}

	app.txPool = state.NewTxMemPool()
	meshAndPoolProjector := pendingtxs.NewMeshAndPoolProjector(mdb, app.txPool)

	appliedTxs, err := database.NewLDBDatabase(filepath.Join(dbStorepath, "appliedTxs"), 0, 0, lg.WithName("appliedTxs"))
	if err != nil {
		return err
	}
	app.closers = append(app.closers, appliedTxs)
	processor := state.NewTransactionProcessor(db, appliedTxs, meshAndPoolProjector, app.txPool, lg.WithName("state"))

	goldenATXID := types.ATXID(types.HexToHash32(app.Config.GoldenATXID))
	if goldenATXID == *types.EmptyATXID {
		app.log.Panic("invalid Golden ATX ID")
	}

	atxdb := activation.NewDB(atxdbstore, idStore, mdb, layersPerEpoch, goldenATXID, validator, app.addLogger(AtxDbLogger, lg))
	beaconProvider := &blocks.EpochBeaconProvider{}

	var msh *mesh.Mesh
	var trtl *tortoise.ThreadSafeVerifyingTortoise
	trtlCfg := tortoise.Config{
		LayerSyze: int(layerSize),
		Database:  mdb,
		Hdist:     app.Config.Hdist,
		Log:       app.addLogger(TrtlLogger, lg),
		Recovered: mdb.PersistentData(),
	}

	trtl = tortoise.NewVerifyingTortoise(trtlCfg)

	if trtlCfg.Recovered {
		msh = mesh.NewRecoveredMesh(mdb, atxdb, app.Config.REWARD, trtl, app.txPool, processor, app.addLogger(MeshLogger, lg))
		go msh.CacheWarmUp(app.Config.LayerAvgSize)
	} else {
		msh = mesh.NewMesh(mdb, atxdb, app.Config.REWARD, trtl, app.txPool, processor, app.addLogger(MeshLogger, lg))
		app.setupGenesis(processor, msh)
	}

	eValidator := blocks.NewBlockEligibilityValidator(layerSize, app.Config.GenesisTotalWeight, layersPerEpoch, atxdb, beaconProvider, BLS381.Verify2, msh, app.addLogger(BlkEligibilityLogger, lg))

	syncConf := sync.Configuration{Concurrency: 4,
		LayerSize:       int(layerSize),
		LayersPerEpoch:  layersPerEpoch,
		RequestTimeout:  time.Duration(app.Config.SyncRequestTimeout) * time.Millisecond,
		SyncInterval:    time.Duration(app.Config.SyncInterval) * time.Second,
		ValidationDelta: time.Duration(app.Config.SyncValidationDelta) * time.Second,
		Hdist:           app.Config.Hdist,
		AtxsLimit:       app.Config.AtxsPerBlock,
		AlwaysListen:    app.Config.AlwaysListen,
		GoldenATXID:     goldenATXID,
	}

	if app.Config.AtxsPerBlock > miner.AtxsPerBlockLimit { // validate limit
		app.log.Panic("Number of atxs per block required is bigger than the limit atxsPerBlock=%v limit=%v", app.Config.AtxsPerBlock, miner.AtxsPerBlockLimit)
	}

	// we can't have an epoch offset which is greater/equal than the number of layers in an epoch
	if app.Config.HareEligibility.EpochOffset >= app.Config.BaseConfig.LayersPerEpoch {
		app.log.Panic("Epoch offset cannot be greater than or equal to the number of layers per epoch EpochOffset=%v LayersPerEpoch=%v",
			app.Config.HareEligibility.EpochOffset, app.Config.BaseConfig.LayersPerEpoch)
	}

	syncer := sync.NewSync(swarm, msh, app.txPool, atxdb, eValidator, poetDb, syncConf, clock, app.addLogger(SyncLogger, lg))
	blockOracle := blocks.NewMinerBlockOracle(layerSize, app.Config.GenesisTotalWeight, layersPerEpoch, atxdb, beaconProvider, vrfSigner, nodeID, syncer.ListenToGossip, app.addLogger(BlockOracle, lg))

	// TODO: we should probably decouple the apptest and the node (and duplicate as necessary) (#1926)
	var hOracle hare.Rolacle
	if isFixedOracle { // fixed rolacle, take the provided rolacle
		hOracle = rolacle
	} else { // regular oracle, build and use it
		beacon := eligibility.NewBeacon(mdb, app.Config.HareEligibility.ConfidenceParam, app.addLogger(HareBeaconLogger, lg))
		hOracle = eligibility.New(beacon, atxdb.GetMinerWeightsInEpochFromView, BLS381.Verify2, vrfSigner, uint16(app.Config.LayersPerEpoch), app.Config.POST.UnitSize, app.Config.GenesisTotalWeight, mdb, app.Config.HareEligibility, app.addLogger(HareOracleLogger, lg))
	}

	gossipListener := service.NewListener(swarm, syncer, app.addLogger(GossipListener, lg))
	ha := app.HareFactory(mdb, swarm, sgn, nodeID, syncer, msh, hOracle, idStore, clock, lg)

	stateAndMeshProjector := pendingtxs.NewStateAndMeshProjector(processor, msh)
	cfg := miner.Config{
		Hdist:          app.Config.Hdist,
		MinerID:        nodeID,
		AtxsPerBlock:   app.Config.AtxsPerBlock,
		LayersPerEpoch: layersPerEpoch,
		TxsPerBlock:    app.Config.TxsPerBlock,
	}

	database.SwitchCreationContext(dbStorepath, "") // currently only blockbuilder uses this mechanism
	blockProducer := miner.NewBlockBuilder(cfg, sgn, swarm, clock.Subscribe(), coinToss, msh, trtl, ha, blockOracle, syncer, stateAndMeshProjector, app.txPool, atxdb, app.addLogger(BlockBuilderLogger, lg))

	bCfg := blocks.Config{
		Depth:       app.Config.Hdist,
		GoldenATXID: goldenATXID,
	}
	blockListener := blocks.NewBlockHandler(bCfg, msh, eValidator, lg)

	poetListener := activation.NewPoetListener(swarm, poetDb, app.addLogger(PoetListenerLogger, lg))

	postMgr, err := activation.NewPostManager(util.Hex2Bytes(nodeID.Key), app.Config.POST, store, app.addLogger(PostLogger, lg))
	if err != nil {
		app.log.Panic("Failed to create post manager: %v", err)
	}

	nipostBuilder := activation.NewNIPoSTBuilder(util.Hex2Bytes(nodeID.Key), postMgr, poetClient, poetDb, store, app.addLogger(NipostBuilderLogger, lg))

	// MERGE FIX
	//if coinBase.Big().Uint64() == 0 && app.Config.StartMining {
	//	app.log.Panic("invalid coinbase account")
	//}
	//if app.Config.SpaceToCommit == 0 {
	//	app.Config.SpaceToCommit = app.Config.POST.SpacePerUnit
	//}
	//

	coinbaseAddr := types.HexToAddress(app.Config.CoinbaseAccount)
	if app.Config.StartSmeshing {
		if coinbaseAddr.Big().Uint64() == 0 {
			app.log.Panic("invalid Coinbase account")
		}
	}

	builderConfig := activation.Config{
		CoinbaseAccount: coinbaseAddr,
		GoldenATXID:     goldenATXID,
		LayersPerEpoch:  layersPerEpoch,
	}

	atxBuilder := activation.NewBuilder(builderConfig, nodeID, app.Config.SpaceToCommit, sgn, atxdb, swarm, msh, layersPerEpoch, nipostBuilder, postMgr, clock, syncer, store, app.addLogger("atxBuilder", lg))

	gossipListener.AddListener(state.IncomingTxProtocol, priorityq.Low, processor.HandleTxData)
	gossipListener.AddListener(activation.AtxProtocol, priorityq.Low, atxdb.HandleGossipAtx)
	gossipListener.AddListener(blocks.NewBlockProtocol, priorityq.High, blockListener.HandleBlock)

	app.blockProducer = blockProducer
	app.blockListener = blockListener
	app.gossipListener = gossipListener
	app.mesh = msh
	app.syncer = syncer
	app.clock = clock
	app.state = processor
	app.hare = ha
	app.P2P = swarm
	app.poetListener = poetListener
	app.atxBuilder = atxBuilder
	app.postMgr = postMgr
	app.oracle = blockOracle
	app.txProcessor = processor
	app.atxDb = atxdb

	return nil
}

// periodically checks that our clock is sync
func (app *SpacemeshApp) checkTimeDrifts() {
	checkTimeSync := time.NewTicker(app.Config.TIME.RefreshNtpInterval)
	defer checkTimeSync.Stop() // close ticker

	for {
		select {
		case <-app.term:
			return

		case <-checkTimeSync.C:
			_, err := timesync.CheckSystemClockDrift()
			if err != nil {
				app.log.Error("System time couldn't synchronize %s", err)
				cmdp.Cancel()
				return
			}
		}
	}
}

// HareFactory returns a hare consensus algorithm according to the parameters is app.Config.Hare.SuperHare
func (app *SpacemeshApp) HareFactory(mdb *mesh.DB, swarm service.Service, sgn hare.Signer, nodeID types.NodeID, syncer *sync.Syncer, msh *mesh.Mesh, hOracle hare.Rolacle, idStore *activation.IdentityStore, clock TickProvider, lg log.Log) HareService {
	if app.Config.HARE.SuperHare {
		hr := turbohare.New(msh)
		mdb.InputVectorBackupFunc = hr.GetResult
		return hr
	}

	// a function to validate we know the blocks
	validationFunc := func(ids []types.BlockID) bool {
		for _, b := range ids {
			res, err := mdb.GetBlock(b)
			if err != nil {
				app.log.With().Error("output set block not in database", b, log.Err(err))
				return false
			}
			if res == nil {
				app.log.With().Error("output set block not in database (BUG BUG BUG - FetchBlock return err nil and res nil)", b)
				return false
			}

		}

		return true
	}
	ha := hare.New(app.Config.HARE, swarm, sgn, nodeID, validationFunc, syncer.IsHareSynced, msh, hOracle, uint16(app.Config.LayersPerEpoch), idStore, hOracle, clock.Subscribe(), app.addLogger(HareLogger, lg))
	return ha
}

func (app *SpacemeshApp) startServices() {
	//app.blockListener.Start()
	go app.startSyncer()

	err := app.hare.Start()
	if err != nil {
		log.Panic("cannot start hare")
	}
	err = app.blockProducer.Start()
	if err != nil {
		log.Panic("cannot start block producer")
	}

	app.poetListener.Start()

	if app.Config.StartSmeshing {
		coinbaseAddr := types.HexToAddress(app.Config.CoinbaseAccount)
		go func() {
			if completedChan, ok := app.postMgr.InitCompleted(); !ok {
				doneChan, err := app.postMgr.CreatePostData(&app.Config.PostOptions)
				if err != nil {
					log.Panic("Failed to create post data: %v", err)
				}
				<-doneChan

				// if completedChan isn't closed then the session failed
				// and we can't start smeshing.
				select {
				case <-completedChan:
				default:
					return
				}
			}

			if err := app.atxBuilder.StartSmeshing(coinbaseAddr); err != nil {
				log.Panic("Failed to start smeshing: %v", err)
			}
		}()
	} else {
		log.Info("Smeshing not started. waiting to be started via smesher API")
	}

	app.clock.StartNotifying()
	go app.checkTimeDrifts()
}

func (app *SpacemeshApp) startAPIServices(net api.NetworkAPI) {
	apiConf := &app.Config.API
	layerDuration := app.Config.LayerDurationSec
	// MERGE FIX -- PROBABLY NOT NEEDED ANYMORE
	//if apiConf.StartGrpcServer || apiConf.StartJSONServer {
	//	// start grpc if specified or if json rpc specified
	//	app.grpcAPIService = api.NewGrpcService(apiConf.GrpcServerPort, net, app.state, app.mesh, app.txPool,
	//		app.oracle, app.clock, layerDuration, app.syncer, app.Config, app)
	//	app.grpcAPIService.StartService()
	//}

	// API SERVICES
	// Since we have multiple GRPC services, we cannot automatically enable them if
	// the gateway server is enabled (since we don't know which ones to enable), so
	// it's an error if the gateway server is enabled without enabling at least one
	// GRPC service.

	// Make sure we only create the server once.
	registerService := func(svc grpcserver.ServiceAPI) {
		if app.grpcAPIService == nil {
			app.grpcAPIService = grpcserver.NewServerWithInterface(apiConf.GrpcServerPort, apiConf.GrpcServerInterface)
		}
		svc.RegisterService(app.grpcAPIService)
	}

	// Register the requested services one by one
	if apiConf.StartDebugService {
		registerService(grpcserver.NewDebugService(app.mesh))
	}
	if apiConf.StartGatewayService {
		registerService(grpcserver.NewGatewayService(net))
	}
	if apiConf.StartGlobalStateService {
		registerService(grpcserver.NewGlobalStateService(app.mesh, app.txPool))
	}
	if apiConf.StartMeshService {
		registerService(grpcserver.NewMeshService(app.mesh, app.txPool, app.clock, app.Config.LayersPerEpoch, app.Config.P2P.NetworkID, layerDuration, app.Config.LayerAvgSize, app.Config.TxsPerBlock))
	}
	if apiConf.StartNodeService {
		registerService(grpcserver.NewNodeService(net, app.mesh, app.clock, app.syncer))
	}
	if apiConf.StartSmesherService {
		registerService(grpcserver.NewSmesherService(app.postMgr, app.atxBuilder))
	}
	if apiConf.StartTransactionService {
		registerService(grpcserver.NewTransactionService(net, app.mesh, app.txPool, app.syncer))
	}

	// Now that the services are registered, start the server.
	if app.grpcAPIService != nil {
		app.grpcAPIService.Start()
	}

	if apiConf.StartJSONServer {
		if app.grpcAPIService == nil {
			// This panics because it should not happen.
			// It should be caught inside apiConf.
			log.Panic("one or more new GRPC services must be enabled with new JSON gateway server.")
		}
		app.jsonAPIService = grpcserver.NewJSONHTTPServer(apiConf.JSONServerPort, apiConf.GrpcServerPort)
		app.jsonAPIService.StartService(
			apiConf.StartDebugService,
			apiConf.StartGatewayService,
			apiConf.StartGlobalStateService,
			apiConf.StartMeshService,
			apiConf.StartNodeService,
			apiConf.StartSmesherService,
			apiConf.StartTransactionService,
		)
	}
}

func (app *SpacemeshApp) stopServices() {
	// all go-routines that listen to app.term will close
	// note: there is no guarantee that a listening go-routine will close before stopServices exits
	close(app.term)

	if app.jsonAPIService != nil {
		log.Info("stopping JSON gateway service...")
		if err := app.jsonAPIService.Close(); err != nil {
			log.Error("error stopping JSON gateway server: %s", err)
		}
	}

	if app.grpcAPIService != nil {
		log.Info("Stopping GRPC service...")
		// does not return any errors
		app.grpcAPIService.Close()
	}

	// MERGE FIX
	//if app.newjsonAPIService != nil {
	//	log.Info("Stopping new JSON gateway service...")
	//	app.newjsonAPIService.Close()
	//}
	//
	//if app.newgrpcAPIService != nil {
	//	log.Info("Stopping new grpc service...")
	//	app.newgrpcAPIService.Close()
	//}

	if app.postMgr != nil {
		_ = app.postMgr.StopPostDataCreationSession(false)
	}

	if app.blockProducer != nil {
		app.log.Info("%v closing block producer", app.nodeID.Key)
		if err := app.blockProducer.Close(); err != nil {
			log.Error("cannot stop block producer %v", err)
		}
	}

	if app.clock != nil {
		app.log.Info("%v closing clock", app.nodeID.Key)
		app.clock.Close()
	}

	if app.poetListener != nil {
		app.log.Info("closing PoET listener")
		app.poetListener.Close()
	}

	if app.atxBuilder != nil {
		app.log.Info("closing atx builder")
		_ = app.atxBuilder.StopSmeshing()
	}

	/*if app.blockListener != nil {
		app.log.Info("%v closing blockListener", app.nodeID.Key)
		app.blockListener.Close()
	}*/

	if app.hare != nil {
		app.log.Info("%v closing Hare", app.nodeID.Key)
		app.hare.Close()
	}

	if app.P2P != nil {
		app.log.Info("%v closing p2p", app.nodeID.Key)
		app.P2P.Shutdown()
	}

	if app.syncer != nil {
		app.log.Info("%v closing sync", app.nodeID.Key)
		app.syncer.Close()
	}

	if app.mesh != nil {
		app.log.Info("%v closing mesh", app.nodeID.Key)
		app.mesh.Close()
	}

	if app.gossipListener != nil {
		app.gossipListener.Stop()
	}

	events.CloseEventReporter()
	events.CloseEventPubSub()
	// Close all databases.
	for _, closer := range app.closers {
		if closer != nil {
			closer.Close()
		}
	}
}

// LoadOrCreateEdSigner either loads a previously created ed identity for the node or creates a new one if not exists
func (app *SpacemeshApp) LoadOrCreateEdSigner() (*signing.EdSigner, error) {
	filename := filepath.Join(app.Config.POST.DataDir, edKeyFileName)
	log.Info("Looking for identity file at `%v`", filename)

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read identity file: %v", err)
			// MERGE FIX
			// log.With().Warning("failed to find identity file", log.Err(err))
		}

		log.Info("Identity file not found. Creating new identity...")

		edSgn := signing.NewEdSigner()
		err := os.MkdirAll(filepath.Dir(filename), filesystem.OwnerReadWriteExec)
		if err != nil {
			return nil, fmt.Errorf("failed to create directory for identity file: %v", err)
		}
		err = ioutil.WriteFile(filename, edSgn.ToBuffer(), filesystem.OwnerReadWrite)
		if err != nil {
			return nil, fmt.Errorf("failed to write identity file: %v", err)
		}

		log.With().Warning("created new identity", edSgn.PublicKey())
		return edSgn, nil
	}

	edSgn, err := signing.NewEdSignerFromBuffer(data)
	if err != nil {
		return nil, fmt.Errorf("failed to construct identity from data file: %v", err)
	}

	// MERGE FIX
	//if edSgn.PublicKey().String() != filepath.Base(filepath.Dir(f)) {
	//	return nil, fmt.Errorf("identity file path ('%s') does not match public key (%s)", filepath.Dir(f), edSgn.PublicKey().String())
	//}
	//log.With().Info("loaded identity from file", log.String("file", f))
	log.Info("Loaded existing identity; public key: %v", edSgn.PublicKey())

	return edSgn, nil
}

type identityFileFound struct{}

func (identityFileFound) Error() string {
	return "identity file found"
}

func (app *SpacemeshApp) getIdentityFile() (string, error) {
	var f string
	err := filepath.Walk(app.Config.POST.DataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() && info.Name() == edKeyFileName {
			f = path
			return &identityFileFound{}
		}
		return nil
	})
	if _, ok := err.(*identityFileFound); ok {
		return f, nil
	}
	if err != nil {
		return "", fmt.Errorf("failed to traverse PoST data dir: %v", err)
	}
	return "", fmt.Errorf("not found")
}

func (app *SpacemeshApp) startSyncer() {
	if app.P2P == nil {
		app.log.Error("syncer started before P2P is initialized")
	} else {
		<-app.P2P.GossipReady()
	}
	app.syncer.Start()
}

// Start starts the Spacemesh node and initializes all relevant services according to command line arguments provided.
func (app *SpacemeshApp) Start(cmd *cobra.Command, args []string) {
	log.With().Info("starting Spacemesh", log.String("data-dir", app.Config.DataDir()), log.String("post-dir", app.Config.POST.DataDir))

	err := filesystem.ExistOrCreate(app.Config.DataDir())
	if err != nil {
		log.Error("data-dir not found or could not be created err:%v", err)
	}

	/* Setup monitoring */

	if app.Config.MemProfile != "" {
		log.Info("Starting mem profiling")
		f, err := os.Create(app.Config.MemProfile)
		if err != nil {
			log.Error("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Error("could not write memory profile: ", err)
		}
	}

	if app.Config.CPUProfile != "" {
		log.Info("Starting cpu profile")
		f, err := os.Create(app.Config.CPUProfile)
		if err != nil {
			log.Error("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Error("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	if app.Config.PprofHTTPServer {
		log.Info("Starting pprof server")
		srv := &http.Server{Addr: ":6060"}
		defer srv.Shutdown(context.TODO())
		go func() {
			err := srv.ListenAndServe()
			if err != nil {
				log.Error("cannot start http server", err)
			}
		}()
	}

	/* Create or load miner identity */

	app.edSgn, err = app.LoadOrCreateEdSigner()
	if err != nil {
		log.Panic("could not retrieve identity err=%v", err)
	}

	poetClient := activation.NewHTTPPoetClient(cmdp.Ctx, app.Config.PoETServer)

	rng := amcl.NewRAND()
	pub := app.edSgn.PublicKey().Bytes()
	rng.Seed(len(pub), app.edSgn.Sign(pub)) // assuming ed.private is random, the sig can be used as seed
	vrfPriv, vrfPub := BLS381.GenKeyPair(rng)
	vrfSigner := BLS381.NewBlsSigner(vrfPriv)
	nodeID := types.NodeID{Key: app.edSgn.PublicKey().String(), VRFPublicKey: vrfPub}

	// This base logger must be debug level so that other, derived loggers are not a lower level.
	lg := log.NewWithLevel(nodeID.ShortString(), zap.NewAtomicLevelAt(zapcore.DebugLevel)).WithFields(nodeID)

	/* Initialize all protocol services */

	dbStorepath := app.Config.DataDir()
	gTime, err := time.Parse(time.RFC3339, app.Config.GenesisTime)
	if err != nil {
		log.With().Error("cannot parse genesis time", log.Err(err))
	}
	ld := time.Duration(app.Config.LayerDurationSec) * time.Second
	clock := timesync.NewClock(timesync.RealClock{}, ld, gTime, log.NewDefault("clock"))

	log.Info("initializing P2P services")
	swarm, err := p2p.New(cmdp.Ctx, app.Config.P2P, app.addLogger(P2PLogger, lg), dbStorepath)
	if err != nil {
		log.Panic("error starting p2p services. err: %v", err)
	}

	err = app.initServices(nodeID, swarm, dbStorepath, app.edSgn, false, nil, uint32(app.Config.LayerAvgSize), poetClient, vrfSigner, uint16(app.Config.LayersPerEpoch), clock)
	if err != nil {
		log.With().Error("cannot start services", log.Err(err))
		return
	}

	if app.Config.CollectMetrics {
		metrics.StartCollectingMetrics(app.Config.MetricsPort)
	}

	app.startServices()

	// P2P must start last to not block when sending messages to protocols
	err = app.P2P.Start()
	if err != nil {
		log.Panic("Error starting p2p services: %v", err)
	}

	app.startAPIServices(app.P2P)
	events.SubscribeToLayers(clock.Subscribe())
	log.Info("App started.")

	// notify anyone who might be listening that the app has finished starting.
	// this can be used by, e.g., app tests.
	close(app.started)

	// app blocks until it receives a signal to exit
	// this signal may come from the node or from sig-abort (ctrl-c)
	<-cmdp.Ctx.Done()
	events.ReportError(events.NodeError{
		Msg:   "node is shutting down",
		Level: zapcore.InfoLevel,
	})
}
