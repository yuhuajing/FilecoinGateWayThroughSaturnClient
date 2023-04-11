package caboose

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	ipfsblockstore "github.com/ipfs/boxo/blockstore"
	ipath "github.com/ipfs/boxo/coreiface/path"
	gateway "github.com/ipfs/boxo/gateway"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

type Config struct {
	// OrchestratorEndpoint is the URL of the Saturn orchestrator.
	//表示Saturn中心节点的URL
	OrchestratorEndpoint *url.URL //"l1s.strn.pl"
	// OrchestratorClient is the HTTP client to use when communicating with the Saturn orchestrator.
	OrchestratorClient *http.Client //default: http.DefaultClient

	// LoggingEndpoint is the URL of the logging endpoint where we submit logs pertaining to our Saturn retrieval requests.
	//client客户端的网址，和L1日志对比进行logs dection
	LoggingEndpoint url.URL
	// LoggingClient is the HTTP client to use when communicating with the logging endpoint.
	LoggingClient *http.Client
	//隔多长时间发送一次client端的检索logs
	// LoggingInterval is the interval at which we submit logs to the logging endpoint.
	LoggingInterval time.Duration

	// SaturnClient is the HTTP client to use when retrieving content from the Saturn network.

	SaturnClient *http.Client
	ExtraHeaders *http.Header
	//是否检验数据
	// DoValidation is used to determine if we should validate the blocks recieved from the Saturn network.
	DoValidation bool

	//每次根据IP地域获取多个L1节点，通过XOR（L1，CID）的异或距离来选择Fetch的节点
	// If set, AffinityKey is used instead of the block CID as the key on the
	// Saturn node pool to determine which Saturn node to retrieve the block from.
	// NOTE: If gateway.ContentPathKey is present in request context,
	// it will be used as AffinityKey automatically.
	AffinityKey string

	// PoolRefresh is the interval at which we refresh the pool of Saturn nodes.
	PoolRefresh time.Duration
	//节点返回检索成功或失败的总时间，节点的信誉积分，根据时间进行评分，决策是否移除节点
	// PoolWeightChangeDebounce is the amount of time we wait between consecutive updates to the weight of a Saturn node
	// in our pool after a retrieval success/failure.
	PoolWeightChangeDebounce time.Duration
	//节点被移除后的冷静时间，冷静期内不能被重新加入Pool
	// PoolMembershipDebounce is the amount of time we wait after a saturn node is removed from the pool
	// before we add it again to the pool.
	PoolMembershipDebounce time.Duration

	// trigger early refreshes when pool size drops below this low watermark
	//pool的下限
	PoolLowWatermark int
	//同一个CID的最大检索次数
	// MaxRetrievalAttempts determines the number of times we will attempt to retrieve a block from the Saturn network before failing.
	MaxRetrievalAttempts int
	//在将CID添加进失败黑名单前允许的失败次数
	// MaxFetchFailuresBeforeCoolDown is the maximum number of retrieval failures across the pool for a key we will tolerate before we
	// add the key to the cool down cache.
	MaxFetchFailuresBeforeCoolDown int

	// FetchKeyCoolDownDuration is duration of time a key will stay in the cool down cache
	// before we start making retrieval attempts for it.
	//CID在失败黑名单中待的时间
	FetchKeyCoolDownDuration time.Duration
	//节点的热冷静期，冷静期内不会对这个节点发起请求
	// SaturnNodeCoolOff is the cool off duration for a saturn node once we determine that we shouldn't be sending requests to it for a while.
	SaturnNodeCoolOff time.Duration
	MinCoolOff        time.Duration
	//失败多少次才会对这个节点添加热冷静期
	// MaxNCoolOff is the number of times we will cool off a node before downvoting it.
	MaxNCoolOff int
}

//client 5s提交一次client端的日志
const DefaultLoggingInterval = 5 * time.Second

//
const DefaultSaturnOrchestratorRequestTimeout = 30 * time.Second

const DefaultSaturnBlockRequestTimeout = 19 * time.Second
const DefaultSaturnCarRequestTimeout = 30 * time.Minute

const DefaultMaxRetries = 3

//节点时间积分机制
const DefaultPoolFailureDownvoteDebounce = 1 * time.Minute

//节点被移除后的冷静时间，15m内不能被重新加入Pool
const DefaultPoolMembershipDebounce = 3 * DefaultPoolRefreshInterval

//最少5个节点
const DefaultPoolLowWatermark = 5

const maxBlockSize = 4194305 // 4 Mib + 1 byte
const DefaultOrchestratorEndpoint = "https://orchestrator.strn.pl/nodes/nearby?count=1000"
const DefaultPoolRefreshInterval = 5 * time.Minute

// we cool off sending requests to Saturn for a cid for a certain duration
// if we've seen a certain number of failures for it already in a given duration.
// NOTE: before getting creative here, make sure you dont break end user flow
// described in https://github.com/ipni/storetheindex/pull/1344
//在将CID添加进失败黑名单前允许的失败次数
const DefaultMaxFetchFailures = 3 * DefaultMaxRetries // this has to fail more than DefaultMaxRetries done for a single gateway request
//CID在失败黑名单中待的时间
const DefaultFetchKeyCoolDownDuration = 1 * time.Minute // how long will a sane person wait and stare at blank screen with "retry later" error before hitting F5?

// we cool off sending requests to a Saturn node if it returns transient errors rather than immediately downvoting it;
// however, only upto a certain max number of cool-offs.
const DefaultSaturnNodeCoolOff = 5 * time.Minute
const DefaultMaxNCoolOff = 3

//一个节点最多允许失败3次，失败之后会给节点添加5min的等待期。期间不会给该节点发送request请求
//一个CID最多允许失败GET 9次，超过后，会把CID加入黑名单，加入的时间为1min
//根据节点的检索时间评分，管理节点。分数过低移除出Pool后15min内不能加入pool。每1分钟检查一次pool节点的信誉值

var ErrNotImplemented error = errors.New("not implemented")
var ErrNoBackend error = errors.New("no available saturn backend")
var ErrBackendFailed error = errors.New("saturn backend failed")
var ErrContentProviderNotFound error = errors.New("saturn failed to find content providers")
var ErrSaturnTimeout error = errors.New("saturn backend timed out")

type ErrSaturnTooManyRequests struct {
	Node       string
	retryAfter time.Duration
}

func (e *ErrSaturnTooManyRequests) Error() string {
	return fmt.Sprintf("saturn node %s returned Too Many Requests error, please retry after %s", e.Node, humanRetry(e.retryAfter))
}

func (e *ErrSaturnTooManyRequests) RetryAfter() time.Duration {
	return e.retryAfter
}

type ErrCoolDown struct {
	Cid        cid.Cid
	Path       string
	retryAfter time.Duration
}

func (e *ErrCoolDown) Error() string {
	switch true {
	case e.Cid != cid.Undef && e.Path != "":
		return fmt.Sprintf("multiple saturn retrieval failures seen for CID %q and Path %q, please retry after %s", e.Cid, e.Path, humanRetry(e.retryAfter))
	case e.Path != "":
		return fmt.Sprintf("multiple saturn retrieval failures seen for Path %q, please retry after %s", e.Path, humanRetry(e.retryAfter))
	case e.Cid != cid.Undef:
		return fmt.Sprintf("multiple saturn retrieval failures seen for CID %q, please retry after %s", e.Cid, humanRetry(e.retryAfter))
	default:
		return fmt.Sprintf("multiple saturn retrieval failures for unknown CID/Path (BUG), please retry after %s", humanRetry(e.retryAfter))
	}
}

func (e *ErrCoolDown) RetryAfter() time.Duration {
	return e.retryAfter
}

func humanRetry(d time.Duration) string {
	return d.Truncate(time.Second).String()
}

//返回了部分成功的数据，返回失败的的子CID 这个和range request应该差不多
// ErrPartialResponse can be returned from a DataCallback to indicate that some of the requested resource
// was successfully fetched, and that instead of retrying the full resource, that there are
// one or more more specific resources that should be fetched (via StillNeed) to complete the request.
type ErrPartialResponse struct {
	error
	StillNeed []string
}

func (epr ErrPartialResponse) Error() string {
	if epr.error != nil {
		return fmt.Sprintf("partial response: %s", epr.error.Error())
	}
	return "caboose received a partial response"
}

type Caboose struct {
	config *Config
	pool   *pool
	logger *logger
}

// DataCallback allows for extensible validation of path-retrieved data.
type DataCallback func(resource string, reader io.Reader) error

// NewCaboose sets up a caboose fetcher.
// Note: Caboose is NOT a persistent blockstore and does NOT have an in-memory cache.
// Every request will result in a remote network request.
func NewCaboose(config *Config) (*Caboose, error) {
	if config.FetchKeyCoolDownDuration == 0 { //CID 黑名单时间
		config.FetchKeyCoolDownDuration = DefaultFetchKeyCoolDownDuration
	}
	if config.MaxFetchFailuresBeforeCoolDown == 0 { //在停止向节点发送request之前，允许该节点失败的次数
		config.MaxFetchFailuresBeforeCoolDown = DefaultMaxFetchFailures
	}

	if config.SaturnNodeCoolOff == 0 { //冷静期时间，冷静期内不会像节点发请求
		config.SaturnNodeCoolOff = DefaultSaturnNodeCoolOff
	}
	if config.MinCoolOff == 0 {
		config.MinCoolOff = 1 * time.Minute
	}

	if config.MaxNCoolOff == 0 {
		config.MaxNCoolOff = DefaultMaxNCoolOff
	}

	c := Caboose{
		config: config,
		pool:   newPool(config),
		logger: newLogger(config),
	}
	c.pool.logger = c.logger

	if c.config.SaturnClient == nil {
		c.config.SaturnClient = &http.Client{
			Timeout: DefaultSaturnCarRequestTimeout, //30min
		}
	}
	if c.config.OrchestratorEndpoint == nil {
		var err error
		c.config.OrchestratorEndpoint, err = url.Parse(DefaultOrchestratorEndpoint)
		if err != nil {
			return nil, err
		}
	}

	if c.config.PoolRefresh == 0 {
		c.config.PoolRefresh = DefaultPoolRefreshInterval
	}

	if c.config.PoolWeightChangeDebounce == 0 {
		c.config.PoolWeightChangeDebounce = DefaultPoolFailureDownvoteDebounce
	}
	if c.config.PoolMembershipDebounce == 0 {
		c.config.PoolMembershipDebounce = DefaultPoolMembershipDebounce
	}
	if c.config.PoolLowWatermark == 0 {
		c.config.PoolLowWatermark = DefaultPoolLowWatermark
	}
	if c.config.MaxRetrievalAttempts == 0 {
		c.config.MaxRetrievalAttempts = DefaultMaxRetries
	}

	// start the pool
	c.pool.Start()

	return &c, nil
}

// Caboose is a blockstore.
var _ ipfsblockstore.Blockstore = (*Caboose)(nil)

// GetMemberWeights is for testing ONLY
func (c *Caboose) GetMemberWeights() map[string]int {
	c.pool.lk.RLock()
	defer c.pool.lk.RUnlock()

	return c.pool.endpoints.ToWeights()
}

func (c *Caboose) Close() {
	c.pool.Close()
	c.logger.Close()
}

// Fetch allows fetching car archives by a path of the form `/ipfs/<cid>[/path/to/file]`
func (c *Caboose) Fetch(ctx context.Context, path string, cb DataCallback) error {
	return c.pool.fetchResourceWith(ctx, path, cb, c.getAffinity(ctx))
}

func (c *Caboose) Has(ctx context.Context, it cid.Cid) (bool, error) {
	blk, err := c.pool.fetchBlockWith(ctx, it, c.getAffinity(ctx))
	if err != nil {
		return false, err
	}
	return blk != nil, nil
}

func (c *Caboose) Get(ctx context.Context, it cid.Cid) (blocks.Block, error) {
	blk, err := c.pool.fetchBlockWith(ctx, it, c.getAffinity(ctx))
	if err != nil {
		return nil, err
	}
	return blk, nil
}

// GetSize returns the CIDs mapped BlockSize
func (c *Caboose) GetSize(ctx context.Context, it cid.Cid) (int, error) {
	blk, err := c.pool.fetchBlockWith(ctx, it, c.getAffinity(ctx))
	if err != nil {
		return 0, err
	}
	return len(blk.RawData()), nil
}

func (c *Caboose) getAffinity(ctx context.Context) string {
	// https://github.com/ipfs/bifrost-gateway/issues/53#issuecomment-1442732865
	if affG := ctx.Value(gateway.ContentPathKey); affG != nil {
		contentPath := affG.(ipath.Path).String()

		// Using exact content path seems to work better for initial website loads
		// because it groups all blocks related to the single file,
		// but at the same time spreads files across multiple L1s
		// which removes the risk of specific L1 becoming a cache hot spot for
		// websites with huge DAG like /ipns/en.wikipedia-on-ipfs.org
		return contentPath

		/* TODO: if we ever want to revisit, and group per root CID of entire DAG:
		const contentRootIdx = 2
		if parts := strings.Split(contentPath, "/"); len(parts) > contentRootIdx {
			// use top level contentRoot ('id' from /ipfs/id or /ipns/id) as affinity key
			return parts[contentRootIdx]
		}
		*/
	}
	if affC := ctx.Value(c.config.AffinityKey); affC != nil {
		return affC.(string)
	}
	return ""
}

// HashOnRead specifies if every read block should be
// rehashed to make sure it matches its CID.
func (c *Caboose) HashOnRead(enabled bool) {
	c.config.DoValidation = enabled
}

/* Mutable blockstore methods */
func (c *Caboose) Put(context.Context, blocks.Block) error {
	return ErrNotImplemented
}

func (c *Caboose) PutMany(context.Context, []blocks.Block) error {
	return ErrNotImplemented
}
func (c *Caboose) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, ErrNotImplemented
}
func (c *Caboose) DeleteBlock(context.Context, cid.Cid) error {
	return ErrNotImplemented
}

var _ ipfsblockstore.Blockstore = (*Caboose)(nil)
