package sync

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spacemeshos/go-spacemesh/common/types"
	"github.com/spacemeshos/go-spacemesh/database"
	"github.com/spacemeshos/go-spacemesh/events"
	"github.com/spacemeshos/go-spacemesh/log"
	"github.com/spacemeshos/go-spacemesh/mesh"
	p2pconf "github.com/spacemeshos/go-spacemesh/p2p/config"
	p2ppeers "github.com/spacemeshos/go-spacemesh/p2p/peers"
	"github.com/spacemeshos/go-spacemesh/p2p/server"
	"github.com/spacemeshos/go-spacemesh/p2p/service"
	"github.com/spacemeshos/go-spacemesh/rand"
	"github.com/spacemeshos/go-spacemesh/timesync"
)

type forBlockInView func(view map[types.BlockID]struct{}, layer types.LayerID, blockHandler func(block *types.Block) (bool, error)) error

type txMemPool interface {
	Get(id types.TransactionID) (*types.Transaction, error)
	Put(id types.TransactionID, item *types.Transaction)
}

type atxDB interface {
	ProcessAtx(atx *types.ActivationTx) error
	GetFullAtx(id types.ATXID) (*types.ActivationTx, error)
	GetEpochAtxs(epochID types.EpochID) (atxs []types.ATXID)
}

type poetDb interface {
	HasProof(proofRef []byte) bool
	ValidateAndStore(proofMessage *types.PoetProofMessage) error
	GetProofMessage(proofRef []byte) ([]byte, error)
}

type blockEligibilityValidator interface {
	BlockSignedAndEligible(block *types.Block) (bool, error)
}

type ticker interface {
	Subscribe() timesync.LayerTimer
	Unsubscribe(timer timesync.LayerTimer)
	GetCurrentLayer() types.LayerID
	LayerToTime(types.LayerID) time.Time
}

type net struct {
	peers
	RequestTimeout time.Duration
	*server.MessageServer
	exit chan struct{}
}

func (ms net) Close() {
	ms.MessageServer.Close()
	ms.peers.Close()
}

func (ms net) GetTimeout() time.Duration {
	return ms.RequestTimeout
}

func (ms net) GetExit() chan struct{} {
	return ms.exit
}

// Configuration represents all config params needed by syncer
type Configuration struct {
	LayersPerEpoch  uint16
	Concurrency     int // number of workers for sync method
	LayerSize       int
	RequestTimeout  time.Duration
	SyncInterval    time.Duration
	ValidationDelta time.Duration
	AtxsLimit       int
	Hdist           int
	AlwaysListen    bool
	GoldenATXID     types.ATXID
}

var (
	errDupTx           = errors.New("duplicate TransactionID in block")
	errDupAtx          = errors.New("duplicate ATXID in block")
	errNoBlocksInLayer = errors.New("layer has no blocks")
	errNoActiveSet     = errors.New("block does not declare active set")
	errZeroActiveSet   = errors.New("block declares empty active set")
	errInvalidATXID    = errors.New("invalid ATXID")

	emptyLayer = types.Layer{}.Hash()
)

type status int

func (s *status) String() string {
	if *s == 0 {
		return "pending"
	}
	if *s == 1 {
		return "inProgress"
	}
	if *s == 3 {
		return "inProgress2"
	}

	return "done"
}

const (
	pending     status = 0
	inProgress  status = 1
	done        status = 2
	inProgress2 status = 3

	blockMsg      server.MessageType = 1
	layerHashMsg  server.MessageType = 2
	layerIdsMsg   server.MessageType = 3
	txMsg         server.MessageType = 4
	atxMsg        server.MessageType = 5
	poetMsg       server.MessageType = 6
	atxIdsMsg     server.MessageType = 7
	atxIdrHashMsg server.MessageType = 8
	inputVecMsg   server.MessageType = 9

	syncProtocol                      = "/sync/1.0/"
	validatingLayerNone types.LayerID = 0
)

// Syncer is used to sync the node with the network
// periodically the Syncer will check if the node is synced with the rest of the network
// and will follow the sync protocol in order to fetch all missing data and become synced again
type Syncer struct {
	log.Log
	Configuration
	*mesh.Mesh
	blockEligibilityValidator
	*net
	ticker

	poetDb poetDb
	txpool txMemPool
	atxDb  atxDB

	validatingLayer      types.LayerID
	validatingLayerMutex sync.Mutex
	syncLock             types.TryMutex
	startLock            types.TryMutex
	forceSync            chan bool
	syncTimer            *time.Ticker
	exit                 chan struct{}
	gossipLock           sync.RWMutex
	gossipSynced         status
	awaitCh              chan struct{}

	blockQueue *blockQueue
	txQueue    *txQueue
	atxQueue   *atxQueue
}

var _ service.Fetcher = (*Syncer)(nil)

// NewSync fires a sync every sm.SyncInterval or on force space from outside
func NewSync(srv service.Service, layers *mesh.Mesh, txpool txMemPool, atxDB atxDB, bv blockEligibilityValidator, poetdb poetDb, conf Configuration, clock ticker, logger log.Log) *Syncer {

	exit := make(chan struct{})

	srvr := &net{
		RequestTimeout: conf.RequestTimeout,
		MessageServer:  server.NewMsgServer(srv.(server.Service), syncProtocol, conf.RequestTimeout, make(chan service.DirectMessage, p2pconf.Values.BufferSize), logger),
		peers:          p2ppeers.NewPeers(srv, logger.WithName("peers")),
		exit:           exit,
	}

	s := &Syncer{
		blockEligibilityValidator: bv,
		Configuration:             conf,
		Log:                       logger,
		Mesh:                      layers,
		net:                       srvr,
		ticker:                    clock,
		syncLock:                  types.TryMutex{},
		poetDb:                    poetdb,
		txpool:                    txpool,
		atxDb:                     atxDB,
		startLock:                 types.TryMutex{},
		forceSync:                 make(chan bool),
		validatingLayer:           validatingLayerNone,
		syncTimer:                 time.NewTicker(conf.SyncInterval),
		exit:                      exit,
		gossipSynced:              pending,
		awaitCh:                   make(chan struct{}),
	}

	s.blockQueue = newValidationQueue(srvr, conf, s)
	s.txQueue = newTxQueue(s)
	s.atxQueue = newAtxQueue(s, s.FetchPoetProof)
	srvr.RegisterBytesMsgHandler(layerHashMsg, newLayerHashRequestHandler(layers, logger))
	srvr.RegisterBytesMsgHandler(blockMsg, newBlockRequestHandler(layers, logger))
	srvr.RegisterBytesMsgHandler(layerIdsMsg, newLayerBlockIdsRequestHandler(layers, logger))
	srvr.RegisterBytesMsgHandler(txMsg, newTxsRequestHandler(s, logger))
	srvr.RegisterBytesMsgHandler(atxMsg, newAtxsRequestHandler(s, logger))
	srvr.RegisterBytesMsgHandler(poetMsg, newPoetRequestHandler(s, logger))
	srvr.RegisterBytesMsgHandler(atxIdsMsg, newEpochAtxsRequestHandler(s, logger))
	srvr.RegisterBytesMsgHandler(atxIdrHashMsg, newAtxHashRequestHandler(s, logger))
	srvr.RegisterBytesMsgHandler(inputVecMsg, newInputVecRequestHandler(s, logger))

	return s
}

// ForceSync signals syncer to run the synchronise flow
func (s *Syncer) ForceSync() {
	s.forceSync <- true
}

// Close closes all running goroutines
func (s *Syncer) Close() {
	s.Info("Closing syncer")
	s.startLock.Lock()
	close(s.exit)
	close(s.forceSync)
	s.startLock.Unlock()
	s.peers.Close()
	s.syncLock.Lock()
	s.syncLock.Unlock()
	s.MessageServer.Close()
	s.blockQueue.Close()
	s.atxQueue.Close()
	s.txQueue.Close()

	s.Info("sync closed")
}

// check if syncer was closed
func (s *Syncer) isClosed() bool {
	select {
	case <-s.exit:
		s.Info("receive interrupt")
		return true
	default:
		return false
	}
}

// equivalent to s.LatestLayer() >= s.lastTickedLayer()-1
// means we have data from the previous layer
func (s *Syncer) weaklySynced(layer types.LayerID) bool {
	return s.LatestLayer()+1 >= layer
}

func (s *Syncer) getGossipBufferingStatus() status {
	s.gossipLock.RLock()
	b := s.gossipSynced
	s.gossipLock.RUnlock()
	return b
}

// Await returns a channel that blocks until the node is synced
func (s *Syncer) Await() chan struct{} {
	return s.awaitCh
}

func (s *Syncer) notifySubscribers(prevStatus, status status) {
	if (status == done) == (prevStatus == done) {
		return
	}
	if status == done {
		close(s.awaitCh)
	} else {
		s.awaitCh = make(chan struct{})
	}
}

// ListenToGossip enables other modules to check if they should listen to gossip
func (s *Syncer) ListenToGossip() bool {
	return s.AlwaysListen || s.getGossipBufferingStatus() != pending
}

func (s *Syncer) setGossipBufferingStatus(status status) {
	s.gossipLock.Lock()
	defer s.gossipLock.Unlock()
	if status == s.gossipSynced {
		return
	}
	s.Info("setting gossip to '%s' ", status.String())
	s.notifySubscribers(s.gossipSynced, status)
	s.gossipSynced = status

}

// IsSynced returns true if the node is synced false otherwise
func (s *Syncer) IsSynced() bool {
	s.Info("sync state w: %v, g:%v layer : %v latest: %v", s.weaklySynced(s.GetCurrentLayer()), s.getGossipBufferingStatus(), s.GetCurrentLayer(), s.LatestLayer())
	return s.weaklySynced(s.GetCurrentLayer()) && s.getGossipBufferingStatus() == done
}

// IsHareSynced returns true if the hare is synced false otherwise
func (s *Syncer) IsHareSynced() bool {
	return s.getGossipBufferingStatus() == inProgress2 || s.IsSynced()
}

// Start starts the main pooling routine that checks the sync status every set interval
// and calls synchronise if the node is out of sync
func (s *Syncer) Start() {
	if s.startLock.TryLock() {
		defer s.startLock.Unlock()
		if s.isClosed() {
			s.Warning("sync started after closed")
			return
		}
		s.Info("start syncer")
		go s.run()
		s.forceSync <- true
		return
	}
	s.Info("syncer already started")
}

// fires a sync every sm.SyncInterval or on force sync from outside
func (s *Syncer) run() {
	s.Debug("Start running")
	for {
		select {
		case <-s.exit:
			s.Debug("Work stopped")
			return
		case <-s.forceSync:
			go s.synchronise()
		case <-s.syncTimer.C:
			go s.synchronise()
		}
	}
}

func (s *Syncer) synchronise() {
	// only one concurrent synchronise
	if s.syncLock.TryLock() == false {
		return
	}

	// release synchronise lock
	defer s.syncLock.Unlock()
	curr := s.GetCurrentLayer()

	// node is synced and blocks from current layer have already been validated
	if curr == s.ProcessedLayer() {
		s.Debug("node is synced")
		// fully-synced, make sure we listen to p2p
		s.setGossipBufferingStatus(done)
		return
	}

	// we have all the data of the prev layers so we can simply validate
	if s.weaklySynced(curr) {
		s.handleWeaklySynced()
		if err := s.syncEpochActivations(curr.GetEpoch()); err != nil {
			if curr.GetEpoch().IsGenesis() {
				s.With().Info("cannot fetch epoch atxs (expected during genesis)", curr, log.Err(err))
			} else {
				s.With().Error("cannot fetch epoch atxs", curr, log.Err(err))
			}
		}
	} else {
		s.handleNotSynced(s.ProcessedLayer() + 1)
	}
}

func (s *Syncer) handleWeaklySynced() {
	s.With().Info("Node is weakly synced",
		log.FieldNamed("latest_layer", s.LatestLayer()),
		log.FieldNamed("current_layer", s.GetCurrentLayer()),
	)
	events.ReportNodeStatusUpdate()

	// handle all layers from processed+1 to current -1
	s.handleLayersTillCurrent()

	if s.isClosed() {
		return
	}

	// validate current layer if more than s.ValidationDelta has passed
	// TODO: remove this since hare runs it?
	if err := s.handleCurrentLayer(); err != nil {
		s.With().Error("node is out of sync", log.Err(err))
		s.setGossipBufferingStatus(pending)
		return
	}

	if s.isClosed() {
		return
	}

	// fully-synced, make sure we listen to p2p
	s.setGossipBufferingStatus(done)
	s.With().Info("node is synced")
	return
}

// validate all layers except current one
func (s *Syncer) handleLayersTillCurrent() {
	// dont handle current
	if s.ProcessedLayer()+1 >= s.GetCurrentLayer() {
		return
	}

	s.With().Info("handle layers",
		log.FieldNamed("from", s.ProcessedLayer()+1), log.FieldNamed("to", s.GetCurrentLayer()-1))
	for currentSyncLayer := s.ProcessedLayer() + 1; currentSyncLayer < s.GetCurrentLayer(); currentSyncLayer++ {
		if s.isClosed() {
			return
		}
		if err := s.getAndValidateLayer(currentSyncLayer); err != nil {
			if currentSyncLayer.GetEpoch().IsGenesis() {
				s.With().Info("failed getting layer even though we are weakly synced (expected during genesis)",
					log.FieldNamed("currentSyncLayer", currentSyncLayer),
					log.FieldNamed("currentLayer", s.GetCurrentLayer()),
					log.Err(err))
			} else {
				s.Panic("failed getting layer even though we are weakly synced currentLayer=%v lastTicked=%v err=%v", currentSyncLayer, s.GetCurrentLayer(), err)
			}
		}
	}
	return
}

// handle the current consensus layer if its is older than s.ValidationDelta
func (s *Syncer) handleCurrentLayer() error {
	curr := s.GetCurrentLayer()
	if s.LatestLayer() == curr && time.Now().Sub(s.LayerToTime(s.LatestLayer())) > s.ValidationDelta {
		if err := s.getAndValidateLayer(s.LatestLayer()); err != nil {
			if err != database.ErrNotFound {
				s.Panic("failed handling current layer  currentLayer=%v lastTicked=%v err=%v ", s.LatestLayer(), s.GetCurrentLayer(), err)
			}
			if err := s.SetZeroBlockLayer(curr); err != nil {
				return err
			}
		}
	}

	if s.LatestLayer()+1 == curr && curr.GetEpoch().IsGenesis() {
		_, err := s.GetLayer(s.LatestLayer())
		if err == database.ErrNotFound {
			err := s.SetZeroBlockLayer(s.LatestLayer())
			return err
		}
	}
	return nil
}

func (s *Syncer) handleNotSynced(currentSyncLayer types.LayerID) {
	s.Info("Node is out of sync setting gossip-synced to false and starting sync")
	events.ReportNodeStatusUpdate()
	s.setGossipBufferingStatus(pending) // don't listen to gossip while not synced

	// first, bring all the data of the prev layers
	// Note: lastTicked() is not constant but updates as ticks are received
	for ; currentSyncLayer < s.GetCurrentLayer(); currentSyncLayer++ {
		s.With().Info("syncing layer",
			log.FieldNamed("current_sync_layer", currentSyncLayer),
			log.FieldNamed("last_ticked_layer", s.GetCurrentLayer()))

		if s.isClosed() {
			return
		}

		lyr, err := s.getLayerFromNeighbors(currentSyncLayer)
		if err != nil {
			s.With().Info("could not get layer from neighbors", currentSyncLayer, log.Err(err))
			return
		}

		if len(lyr.Blocks()) == 0 {
			if err := s.SetZeroBlockLayer(currentSyncLayer); err != nil {
				s.With().Error("handleNotSynced failed", currentSyncLayer, log.Err(err))
				return
			}
		}
		s.syncAtxs(currentSyncLayer)

		// TODO: implement handling hare terminating with no valid blocks.
		// 	currently hareForLayer is nil if hare hasn't terminated yet.
		//	 ACT: hare should save something in the db when terminating empty set, sync should check it.
		hareForLayer, err := s.DB.GetLayerInputVector(lyr.Index())
		if err != nil {
			s.Log.With().Warning("validating layer without input vector", lyr.Index(), log.Err(err))
		}
		s.ValidateLayer(lyr, hareForLayer) // wait for layer validation
	}

	// wait for two ticks to ensure we are fully synced before we open gossip or validate the current layer
	err := s.gossipSyncForOneFullLayer(currentSyncLayer)
	if err != nil {
		s.With().Error("failed getting layer from db even though we listened to gossip",
			currentSyncLayer,
			log.Err(err))
	}
}

func (s *Syncer) syncAtxs(currentSyncLayer types.LayerID) {
	if currentSyncLayer.GetEpoch() == 0 {
		s.With().Info("skipping ATX sync in epoch 0")
		return
	}
	lastLayerOfEpoch := (currentSyncLayer.GetEpoch() + 1).FirstLayer() - 1
	if currentSyncLayer == lastLayerOfEpoch {
		if err := s.syncEpochActivations(currentSyncLayer.GetEpoch()); err != nil {
			if currentSyncLayer.GetEpoch().IsGenesis() {
				s.With().Info("cannot fetch epoch atxs (expected during genesis)", currentSyncLayer, log.Err(err))
			} else {
				s.With().Error("cannot fetch epoch atxs", currentSyncLayer, log.Err(err))
			}
		}
	}
}

//Waits two ticks (while weakly-synced) in order to ensure that we listened to gossip for one full layer
//after that we are assumed to have all the data required for validation so we can validate and open gossip
// opening gossip in weakly-synced transition us to fully-synced
func (s *Syncer) gossipSyncForOneFullLayer(currentSyncLayer types.LayerID) error {
	// listen to gossip
	// subscribe and wait for two ticks
	s.Info("waiting for two ticks while p2p is open, epoch %v", currentSyncLayer.GetEpoch())
	ch := s.ticker.Subscribe()

	var exit bool
	var flayer types.LayerID

	if flayer, exit = s.waitLayer(ch); exit {
		return fmt.Errorf("cloed while buffering first layer")
	}

	if err := s.syncSingleLayer(currentSyncLayer); err != nil {
		return err
	}

	// get & validate first tick
	if err := s.getAndValidateLayer(currentSyncLayer); err != nil {
		if err != database.ErrNotFound {
			return err
		}
		if err := s.SetZeroBlockLayer(currentSyncLayer); err != nil {
			return err
		}
	}

	//todo: just set hare to listen when inProgress and remove inProgress2
	s.setGossipBufferingStatus(inProgress2)

	if _, done := s.waitLayer(ch); done {
		return fmt.Errorf("cloed while buffering second layer ")
	}

	if err := s.syncSingleLayer(flayer); err != nil {
		return err
	}

	// get & validate second tick
	if err := s.getAndValidateLayer(flayer); err != nil {
		if err != database.ErrNotFound {
			return err
		}
		if err := s.SetZeroBlockLayer(flayer); err != nil {
			return err
		}
	}

	s.ticker.Unsubscribe(ch) // unsub, we won't be listening on this ch anymore
	s.Info("done waiting for ticks and validation. setting gossip true")

	// fully-synced - set gossip -synced to true
	s.setGossipBufferingStatus(done)

	return nil
}

func (s *Syncer) syncSingleLayer(currentSyncLayer types.LayerID) error {
	s.With().Info("syncing single layer", log.FieldNamed("current_sync_layer", currentSyncLayer),
		log.FieldNamed("last_ticked_layer", s.GetCurrentLayer()))

	if s.isClosed() {
		return errors.New("shutdown")
	}

	lyr, err := s.getLayerFromNeighbors(currentSyncLayer)
	if err != nil {
		s.With().Info("could not get layer from neighbors", currentSyncLayer, log.Err(err))
		return err
	}

	if len(lyr.Blocks()) == 0 {
		if err := s.SetZeroBlockLayer(currentSyncLayer); err != nil {
			s.With().Error("handleNotSynced failed ", currentSyncLayer, log.Err(err))
			return err
		}
	}
	s.syncAtxs(currentSyncLayer)
	return nil
}

func (s *Syncer) waitLayer(ch timesync.LayerTimer) (types.LayerID, bool) {
	var l types.LayerID
	select {
	case l = <-ch:
		s.Debug("waited one layer")
	case <-s.exit:
		s.Debug("exit while buffering")
		return l, true
	}
	return l, false
}

func (s *Syncer) getLayerFromNeighbors(currentSyncLayer types.LayerID) (*types.Layer, error) {
	if len(s.peers.GetPeers()) == 0 {
		return nil, fmt.Errorf("no peers ")
	}

	// fetch layer hash from each peer
	s.With().Info("fetch layer hash", currentSyncLayer)
	m, err := s.fetchLayerHashes(currentSyncLayer)
	if err != nil {
		if err == errNoBlocksInLayer {
			return types.NewLayer(currentSyncLayer), nil
		}
		return nil, err
	}

	if s.isClosed() {
		return nil, fmt.Errorf("interupt")
	}

	// fetch ids for each hash
	s.With().Info("fetch layer ids", currentSyncLayer)
	blockIds, err := s.fetchLayerBlockIds(m, currentSyncLayer)
	if err != nil {
		return nil, err
	}

	if s.isClosed() {
		return nil, fmt.Errorf("interupt")
	}

	blocksArr, err := s.syncLayer(currentSyncLayer, blockIds)
	if len(blocksArr) == 0 || err != nil {
		return nil, fmt.Errorf("could not get blocks for layer %v %v", currentSyncLayer, err)
	}

	input, err := s.syncInputVector(currentSyncLayer)
	if err != nil {
		input = nil
	}

	if err := s.DB.SaveLayerInputVector(currentSyncLayer, input); err != nil {
		s.Log.Warning("Could'nt save input vector to db %v", err)
	}

	return types.NewExistingLayer(types.LayerID(currentSyncLayer), blocksArr), nil
}

func (s *Syncer) syncEpochActivations(epoch types.EpochID) error {
	s.With().Info("syncing atxs", epoch)
	hashes, err := s.fetchEpochAtxHashes(epoch)
	if err != nil {
		return err
	}

	atxIds, err := s.fetchEpochAtxs(hashes, epoch)
	if err != nil {
		return err
	}

	s.With().Info("fetched atxs for epoch", epoch, log.Int("count", len(atxIds)))
	s.With().Debug("fetched atxs for epoch",
		epoch,
		log.Int("count", len(atxIds)),
		log.String("atxs", fmt.Sprint(atxIds)))

	_, err = s.atxQueue.HandleAtxs(atxIds)

	return err
}

// GetAtxs fetches list of atxs from remote peers if possible
func (s *Syncer) GetAtxs(IDs []types.ATXID) error {
	_, err := s.atxQueue.HandleAtxs(IDs)
	return err
}

func (s *Syncer) syncLayer(layerID types.LayerID, blockIds []types.BlockID) ([]*types.Block, error) {
	ch := make(chan bool, 1)
	foo := func(res bool) error {
		s.With().Info("sync layer done", layerID)
		ch <- res
		return nil
	}

	tmr := newMilliTimer(syncLayerTime)
	if res, err := s.blockQueue.addDependencies(layerID, blockIds, foo); err != nil {
		return nil, fmt.Errorf("failed adding layer %v blocks to queue %v", layerID, err)
	} else if res == false {
		s.With().Info("no missing blocks for layer", layerID)
		return s.LayerBlocks(layerID)
	}

	s.With().Info("wait for blocks", layerID, log.Int("num_blocks", len(blockIds)), types.BlockIdsField(blockIds))
	select {
	case <-s.exit:
		return nil, fmt.Errorf("received interupt")
	case result := <-ch:
		if !result {
			return nil, fmt.Errorf("could not get all blocks for layer %v", layerID)
		}
	}

	tmr.ObserveDuration()

	return s.LayerBlocks(layerID)
}

func (s *Syncer) syncInputVector(layerID types.LayerID) ([]types.BlockID, error) {
	//tmr := newMilliTimer(syncLaye	Time)
	if r, err := s.DB.GetLayerInputVector(layerID); err == nil {
		return r, nil
	}

	out := <-fetchWithFactory(newNeighborhoodWorker(s, 1, inputVectorReqFactory(layerID.Bytes())))
	if out == nil {
		return nil, fmt.Errorf("could not find input vector with any neighbor")
	}

	inputvec := out.([]types.BlockID)

	//tmr.ObserveDuration()

	return inputvec, nil
}

func (s *Syncer) getBlocks(jobID types.LayerID, blockIds []types.BlockID) error {
	ch := make(chan bool, 1)
	foo := func(res bool) error {
		s.With().Info("get blocks for layer done", jobID)
		ch <- res
		return nil
	}

	tmr := newMilliTimer(syncLayerTime)
	if res, err := s.blockQueue.addDependencies(jobID, blockIds, foo); err != nil {
		return fmt.Errorf("failed adding layer %v blocks to queue %v", jobID, err)
	} else if res == false {
		s.With().Info("no missing blocks for layer", jobID)
		return nil
	}

	s.With().Info("wait for blocks", jobID, log.Int("num_blocks", len(blockIds)))
	select {
	case <-s.exit:
		return fmt.Errorf("received interrupt")
	case result := <-ch:
		if !result {
			return nil
		}
	}

	tmr.ObserveDuration()

	return nil
}

// GetBlocks fetches list of blocks from peers
func (s *Syncer) GetBlocks(blockIds []types.BlockID) error {
	return s.getBlocks(types.LayerID(rand.Int31()), blockIds)
}

func (s *Syncer) fastValidation(block *types.Block) error {
	// block eligibility
	if eligible, err := s.BlockSignedAndEligible(block); err != nil || !eligible {
		return fmt.Errorf("block eligibility check failed - err: %v", err)
	}

	// validate unique tx atx
	if err := validateUniqueTxAtx(block); err != nil {
		return err
	}
	return nil

}

func validateUniqueTxAtx(b *types.Block) error {
	// check for duplicate tx id
	mt := make(map[types.TransactionID]struct{}, len(b.TxIDs))
	for _, tx := range b.TxIDs {
		if _, exist := mt[tx]; exist {
			return errDupTx
		}
		mt[tx] = struct{}{}
	}

	// check for duplicate atx id
	if b.ActiveSet != nil {
		ma := make(map[types.ATXID]struct{}, len(*b.ActiveSet))
		for _, atx := range *b.ActiveSet {
			if _, exist := ma[atx]; exist {
				return errDupAtx
			}
			ma[atx] = struct{}{}
		}
	}

	return nil
}

func (s *Syncer) fetchRefBlock(block *types.Block) error {
	if block.RefBlock == nil {
		return fmt.Errorf("called fetch ref block with nil ref block %v", block.ID())
	}
	_, err := s.GetBlock(*block.RefBlock)
	if err != nil {
		s.With().Info("fetching block", *block.RefBlock)
		fetched := s.fetchBlock(*block.RefBlock)
		if !fetched {
			return fmt.Errorf("failed to fetch ref block %v", *block.RefBlock)
		}
	}
	return nil
}

func (s *Syncer) fetchAllReferencedAtxs(blk *types.Block) error {
	// As block with empty or Golden ATXID is considered syntactically invalid, explicit check is not needed here.
	atxs := []types.ATXID{blk.ATXID}

	if blk.ActiveSet != nil {
		if len(*blk.ActiveSet) > 0 {
			atxs = append(atxs, *blk.ActiveSet...)
		} else {
			return errZeroActiveSet
		}
	} else {
		if blk.RefBlock == nil {
			return errNoActiveSet
		}
	}
	_, err := s.atxQueue.HandleAtxs(atxs)
	return err
}

func (s *Syncer) fetchBlockDataForValidation(blk *types.Block) error {
	if blk.RefBlock != nil {
		err := s.fetchRefBlock(blk)
		if err != nil {
			return err
		}
	}
	return s.fetchAllReferencedAtxs(blk)
}

func (s *Syncer) blockSyntacticValidation(block *types.Block) ([]*types.Transaction, []*types.ActivationTx, error) {
	// A block whose associated ATX is the GoldenATXID or the EmptyATXID - either of these - is syntactically invalid.
	if block.ATXID == *types.EmptyATXID || block.ATXID == s.GoldenATXID {
		return nil, nil, errInvalidATXID
	}

	// validate unique tx atx
	if err := s.fetchBlockDataForValidation(block); err != nil {
		return nil, nil, err
	}

	if err := s.fastValidation(block); err != nil {
		return nil, nil, err
	}

	// data availability
	txs, atxs, err := s.dataAvailability(block)
	if err != nil {
		return nil, nil, fmt.Errorf("DataAvailabilty failed for block %v err: %v", block.ID(), err)
	}

	// validate block's view
	valid := s.validateBlockView(block)
	if valid == false {
		return nil, nil, fmt.Errorf("block %v not syntacticly valid", block.ID())
	}

	return txs, atxs, nil
}

func combineBlockDiffs(blk *types.Block) []types.BlockID {
	return append(append(blk.ForDiff, blk.AgainstDiff...), blk.NeutralDiff...)
}

func (s *Syncer) validateBlockView(blk *types.Block) bool {
	ch := make(chan bool, 1)
	defer close(ch)
	foo := func(res bool) error {
		s.With().Info("view validated",
			blk.ID(),
			log.Bool("result", res),
			blk.LayerIndex)
		ch <- res
		return nil
	}
	if res, err := s.blockQueue.addDependencies(blk.ID(), combineBlockDiffs(blk), foo); err != nil {
		s.Error(fmt.Sprintf("block %v not syntactically valid", blk.ID()), err)
		return false
	} else if res == false {
		s.With().Debug("no missing blocks in view",
			blk.ID(),
			blk.LayerIndex)
		return true
	}

	return <-ch
}

func (s *Syncer) fetchAtx(ID types.ATXID) (*types.ActivationTx, error) {
	atxs, err := s.atxQueue.HandleAtxs([]types.ATXID{ID})
	if err != nil {
		return nil, err
	}
	if len(atxs) == 0 {
		return nil, fmt.Errorf("ATX %v not fetched", ID.ShortString())
	}
	return atxs[0], nil
}

// FetchAtx fetches an ATX from remote peer
func (s *Syncer) FetchAtx(ID types.ATXID) error {
	_, e := s.fetchAtx(ID)
	return e
}

// FetchAtxReferences fetches positioning and prev atxs from peers if they are not found in db
func (s *Syncer) FetchAtxReferences(atx *types.ActivationTx) error {
	if atx.PositioningATX != s.GoldenATXID {
		s.With().Info("going to fetch pos atx", atx.PositioningATX, atx.ID())
		_, err := s.fetchAtx(atx.PositioningATX)
		if err != nil {
			return err
		}
	}

	if atx.PrevATXID != *types.EmptyATXID {
		s.With().Info("going to fetch prev atx", atx.PrevATXID, atx.ID())
		_, err := s.fetchAtx(atx.PrevATXID)
		if err != nil {
			return err
		}
	}
	s.With().Info("done fetching references for atx", atx.ID())

	return nil
}

func (s *Syncer) fetchBlock(ID types.BlockID) bool {
	ch := make(chan bool, 1)
	defer close(ch)
	foo := func(res bool) error {
		s.With().Info("single block fetched",
			ID,
			log.Bool("result", res))
		ch <- res
		return nil
	}
	id := types.CalcHash32(append(ID.Bytes(), []byte(strconv.Itoa(rand.Int()))...))
	if res, err := s.blockQueue.addDependencies(id, []types.BlockID{ID}, foo); err != nil {
		s.Error(fmt.Sprintf("block %v not syntactically valid", ID), err)
		return false
	} else if res == false {
		// block already found
		return true
	}

	return <-ch
}

// FetchBlock fetches a single block from peers
func (s *Syncer) FetchBlock(ID types.BlockID) error {
	if !s.fetchBlock(ID) {
		return fmt.Errorf("stuff")
	}
	return nil
}

func (s *Syncer) dataAvailability(blk *types.Block) ([]*types.Transaction, []*types.ActivationTx, error) {

	wg := sync.WaitGroup{}
	wg.Add(1)
	var txres []*types.Transaction
	var txerr error

	if len(blk.TxIDs) > 0 {
		txres, txerr = s.txQueue.HandleTxs(blk.TxIDs)
	}

	var atxres []*types.ActivationTx

	if txerr != nil {
		return nil, nil, fmt.Errorf("failed fetching block %v transactions %v", blk.ID(), txerr)
	}

	s.With().Info("fetched all block data", blk.Fields()...)
	return txres, atxres, nil
}

// GetTxs fetches txs from peers if necessary
func (s *Syncer) GetTxs(IDs []types.TransactionID) error {
	_, err := s.txQueue.HandleTxs(IDs)
	return err
}

func (s *Syncer) fetchLayerBlockIds(m map[types.Hash32][]p2ppeers.Peer, lyr types.LayerID) ([]types.BlockID, error) {
	// send request to different users according to returned hashes
	idSet := make(map[types.BlockID]struct{}, s.LayerSize)
	ids := make([]types.BlockID, 0, s.LayerSize)
	for h, peers := range m {
	NextHash:
		for _, peer := range peers {
			s.With().Debug("send request", log.String("peer", peer.String()))
			ch, err := layerIdsReqFactory(lyr)(s, peer)
			if err != nil {
				return nil, err
			}

			timeout := time.After(s.Configuration.RequestTimeout)
			select {
			case <-s.GetExit():
				s.Debug("worker received interrupt")
				return nil, fmt.Errorf("interupt")
			case <-timeout:
				s.With().Error("layer ids request timed out", log.String("peer", peer.String()))
				continue
			case v := <-ch:
				if v != nil {
					s.With().Debug("peer responded to layer ids request", log.String("peer", peer.String()))
					// peer returned set with bad hash ask next peer
					res := types.CalcBlocksHash32(v.([]types.BlockID), nil)

					if h != res {
						s.With().Warning("layer ids hash does not match request",
							log.String("peer", peer.String()))
					}

					for _, bid := range v.([]types.BlockID) {
						if _, exists := idSet[bid]; !exists {
							idSet[bid] = struct{}{}
							ids = append(ids, bid)
						}
					}
					// fetch for next hash
					break NextHash
				}
			}
		}
	}

	if len(ids) == 0 {
		s.Info("could not get layer ids from any peer")
	}

	return ids, nil
}

func (s *Syncer) fetchEpochAtxs(m map[types.Hash32][]p2ppeers.Peer, epoch types.EpochID) ([]types.ATXID, error) {
	// send request to different users according to returned hashes
	idSet := make(map[types.ATXID]struct{}, s.LayerSize)
	ids := make([]types.ATXID, 0, s.LayerSize)
	for h, peers := range m {
	NextHash:
		for _, peer := range peers {
			s.With().Debug("send request", log.String("peer", peer.String()))
			ch, err := getEpochAtxIds(epoch, s, peer)
			if err != nil {
				return nil, err
			}

			timeout := time.After(s.Configuration.RequestTimeout)
			select {
			case <-s.GetExit():
				s.Debug("worker received interrupt")
				return nil, fmt.Errorf("interupt")
			case <-timeout:
				s.With().Error("layer ids request timed out", log.String("peer", peer.String()))
				continue
			case v := <-ch:
				if v != nil {
					s.With().Debug("peer responded to epoch atx ids request",
						log.String("peer", peer.String()))
					// peer returned set with bad hash ask next peer
					res := types.CalcATXIdsHash32(v.([]types.ATXID), nil)

					if h != res {
						s.With().Warning("epoch atx ids hash does not match request",
							log.String("peer", peer.String()))
					}

					for _, bid := range v.([]types.ATXID) {
						if _, exists := idSet[bid]; !exists {
							idSet[bid] = struct{}{}
							ids = append(ids, bid)
						}
					}
					// fetch for next hash
					break NextHash
				}
			}
		}
	}

	if len(ids) == 0 {
		s.Info("could not get atx ids from any peer")
	}

	return ids, nil
}

type peerHashPair struct {
	peer p2ppeers.Peer
	hash types.Hash32
}

func (s *Syncer) fetchLayerHashes(lyr types.LayerID) (map[types.Hash32][]p2ppeers.Peer, error) {
	// get layer hash from each peer
	wrk := newPeersWorker(s, s.GetPeers(), &sync.Once{}, hashReqFactory(lyr))
	go wrk.Work()
	m := make(map[types.Hash32][]p2ppeers.Peer)
	layerHasBlocks := false
	for out := range wrk.output {
		pair, ok := out.(*peerHashPair)
		if pair != nil && ok { // do nothing on close channel
			if pair.hash != emptyLayer {
				layerHasBlocks = true
				m[pair.hash] = append(m[pair.hash], pair.peer)
			}

		}
	}

	if !layerHasBlocks {
		s.With().Info("layer has no blocks", lyr)
		return nil, errNoBlocksInLayer
	}

	if len(m) == 0 {
		return nil, errors.New("could not get layer hashes from any peer")
	}
	s.With().Info("layer has blocks", lyr)
	return m, nil
}

func (s *Syncer) fetchEpochAtxHashes(ep types.EpochID) (map[types.Hash32][]p2ppeers.Peer, error) {
	// get layer hash from each peer
	wrk := newPeersWorker(s, s.GetPeers(), &sync.Once{}, atxHashReqFactory(ep))
	go wrk.Work()
	m := make(map[types.Hash32][]p2ppeers.Peer)
	layerHasBlocks := false
	for out := range wrk.output {
		pair, ok := out.(*peerHashPair)
		if pair != nil && ok { // do nothing on close channel
			if pair.hash != emptyLayer {
				layerHasBlocks = true
				m[pair.hash] = append(m[pair.hash], pair.peer)
			}

		}
	}

	if !layerHasBlocks {
		s.With().Info("epoch has no atxs", ep)
		return nil, errNoBlocksInLayer
	}

	if len(m) == 0 {
		return nil, errors.New("could not get epoch hashes from any peer")
	}
	s.With().Info("epoch has atxs", ep)
	return m, nil
}

func fetchWithFactory(wrk worker) chan interface{} {
	// each worker goroutine tries to fetch a block iteratively from each peer
	go wrk.Work()
	for i := 0; int32(i) < atomic.LoadInt32(wrk.workCount)-1; i++ {
		go wrk.Clone().Work()
	}
	return wrk.output
}

// FetchPoetProof fetches a poet proof from network peers
func (s *Syncer) FetchPoetProof(poetProofRef []byte) error {
	if !s.poetDb.HasProof(poetProofRef) {
		out := <-fetchWithFactory(newNeighborhoodWorker(s, 1, poetReqFactory(poetProofRef)))
		if out == nil {
			return fmt.Errorf("could not find PoET proof with any neighbor")
		}
		proofMessage := out.(types.PoetProofMessage)
		err := s.poetDb.ValidateAndStore(&proofMessage)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetPoetProof fetches a poet proof from network peers
func (s *Syncer) GetPoetProof(hash types.Hash32) error {
	poetProofRef := hash.Bytes()
	if !s.poetDb.HasProof(poetProofRef) {
		out := <-fetchWithFactory(newNeighborhoodWorker(s, 1, poetReqFactory(poetProofRef)))
		if out == nil {
			return fmt.Errorf("could not find PoET proof with any neighbor")
		}
		proofMessage := out.(types.PoetProofMessage)
		err := s.poetDb.ValidateAndStore(&proofMessage)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Syncer) atxCheckLocal(atxIds []types.Hash32) (map[types.Hash32]item, map[types.Hash32]item, []types.Hash32) {
	// look in pool
	unprocessedItems := make(map[types.Hash32]item, len(atxIds))
	missingInPool := make([]types.ATXID, 0, len(atxIds))
	for _, t := range atxIds {
		id := types.ATXID(t)
		if x, err := s.atxDb.GetFullAtx(id); err == nil {
			atx := x
			s.With().Debug("found atx in atx db", id)
			unprocessedItems[id.Hash32()] = atx
		} else {
			s.With().Debug("atx not in atx pool", id)
			missingInPool = append(missingInPool, id)
		}
	}
	// look in db
	dbAtxs, missing := s.GetATXs(missingInPool)

	dbItems := make(map[types.Hash32]item, len(dbAtxs))
	for i, k := range dbAtxs {
		dbItems[i.Hash32()] = k
	}

	missingItems := make([]types.Hash32, 0, len(missing))
	for _, i := range missing {
		missingItems = append(missingItems, i.Hash32())
	}

	return unprocessedItems, dbItems, missingItems
}

func (s *Syncer) txCheckLocal(txIds []types.Hash32) (map[types.Hash32]item, map[types.Hash32]item, []types.Hash32) {
	// look in pool
	unprocessedItems := make(map[types.Hash32]item)
	missingInPool := make([]types.TransactionID, 0)
	for _, t := range txIds {
		id := types.TransactionID(t)
		if tx, err := s.txpool.Get(id); err == nil {
			s.With().Debug("found tx in tx pool", id)
			unprocessedItems[id.Hash32()] = tx
		} else {
			s.With().Debug("tx not in tx pool", id)
			missingInPool = append(missingInPool, id)
		}
	}
	// look in db
	dbTxs, missing := s.GetTransactions(missingInPool)

	dbItems := make(map[types.Hash32]item, len(dbTxs))
	for _, k := range dbTxs {
		dbItems[k.Hash32()] = k
	}

	missingItems := make([]types.Hash32, 0, len(missing))
	for i := range missing {
		missingItems = append(missingItems, i.Hash32())
	}

	return unprocessedItems, dbItems, missingItems
}

func (s *Syncer) blockCheckLocal(blockIds []types.Hash32) (map[types.Hash32]item, map[types.Hash32]item, []types.Hash32) {
	// look in pool
	dbItems := make(map[types.Hash32]item)
	for _, id := range blockIds {
		res, err := s.GetBlock(types.BlockID(id.ToHash20()))
		if err != nil {
			s.With().Debug("get block failed", log.String("id", id.ShortString()))
			continue
		}
		dbItems[id] = res
	}

	return nil, dbItems, nil
}

func (s *Syncer) getAndValidateLayer(id types.LayerID) error {
	s.validatingLayerMutex.Lock()
	s.validatingLayer = id
	defer func() {
		s.validatingLayer = validatingLayerNone
		s.validatingLayerMutex.Unlock()
	}()

	lyr, err := s.GetLayer(id)
	if err != nil {
		return err
	}

	// TODO: Get hare results a.k.a input vector from - db/hare and replace this
	inputVector, err := s.DB.GetLayerInputVector(id)
	if err != nil {
		inputVector = nil
	}

	s.Log.With().Info("getAndValidateLayer ", id.Field(), log.String("input_vector", fmt.Sprint(inputVector)), log.String("blocks", fmt.Sprint(types.BlockIDs(lyr.Blocks()))))

	s.ValidateLayer(lyr, inputVector) // wait for layer validation
	return nil
}

func (s *Syncer) getValidatingLayer() types.LayerID {
	return s.validatingLayer
}
