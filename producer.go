package nsq

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrTopicNotSet = errors.New("topic is not set as producer")
	ErrNoProducer  = errors.New("topic producer not found")
)

type producerConn interface {
	String() string
	SetLogger(logger, LogLevel, string)
	Connect() (*IdentifyResponse, error)
	Close() error
	WriteCommand(*Command) error
}

// Producer is a high-level type to publish to NSQ.
//
// A Producer instance is 1:1 with a destination `nsqd`
// and will lazily connect to that instance (and re-connect)
// when Publish commands are executed.
type Producer struct {
	id     int64
	addr   string
	conn   producerConn
	config Config

	logger   logger
	logLvl   LogLevel
	logGuard sync.RWMutex

	responseChan chan []byte
	errorChan    chan []byte
	closeChan    chan int

	transactionChan chan *ProducerTransaction
	transactions    []*ProducerTransaction
	state           int32

	concurrentProducers int32
	stopFlag            int32
	exitChan            chan int
	wg                  sync.WaitGroup
	guard               sync.Mutex
}

// ProducerTransaction is returned by the async publish methods
// to retrieve metadata about the command after the
// response is received.
type ProducerTransaction struct {
	cmd      *Command
	doneChan chan *ProducerTransaction
	Error    error         // the error (or nil) of the publish command
	Args     []interface{} // the slice of variadic arguments passed to PublishAsync or MultiPublishAsync
}

func (t *ProducerTransaction) finish() {
	if t.doneChan != nil {
		t.doneChan <- t
	}
}

// NewProducer returns an instance of Producer for the specified address
//
// The only valid way to create a Config is via NewConfig, using a struct literal will panic.
// After Config is passed into NewProducer the values are no longer mutable (they are copied).
func NewProducer(addr string, config *Config) (*Producer, error) {
	config.assertInitialized()
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	p := &Producer{
		id: atomic.AddInt64(&instCount, 1),

		addr:   addr,
		config: *config,

		logger: log.New(os.Stderr, "", log.Flags()),
		logLvl: LogLevelInfo,

		transactionChan: make(chan *ProducerTransaction),
		exitChan:        make(chan int),
		responseChan:    make(chan []byte),
		errorChan:       make(chan []byte),
	}
	return p, nil
}

// Ping causes the Producer to connect to it's configured nsqd (if not already
// connected) and send a `Nop` command, returning any error that might occur.
//
// This method can be used to verify that a newly-created Producer instance is
// configured correctly, rather than relying on the lazy "connect on Publish"
// behavior of a Producer.
func (w *Producer) Ping() error {
	if atomic.LoadInt32(&w.state) != StateConnected {
		err := w.connect()
		if err != nil {
			return err
		}
	}

	return w.conn.WriteCommand(Nop())
}

// SetLogger assigns the logger to use as well as a level
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//    Output(calldepth int, s string)
//
func (w *Producer) SetLogger(l logger, lvl LogLevel) {
	w.logGuard.Lock()
	defer w.logGuard.Unlock()

	w.logger = l
	w.logLvl = lvl
}

func (w *Producer) getLogger() (logger, LogLevel) {
	w.logGuard.RLock()
	defer w.logGuard.RUnlock()

	return w.logger, w.logLvl
}

// String returns the address of the Producer
func (w *Producer) String() string {
	return w.addr
}

// Stop initiates a graceful stop of the Producer (permanent)
//
// NOTE: this blocks until completion
func (w *Producer) Stop() {
	w.guard.Lock()
	if !atomic.CompareAndSwapInt32(&w.stopFlag, 0, 1) {
		w.guard.Unlock()
		return
	}
	w.log(LogLevelInfo, "stopping")
	close(w.exitChan)
	w.close()
	w.guard.Unlock()
	w.wg.Wait()
}

// PublishAsync publishes a message body to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (w *Producer) PublishAsync(topic string, body []byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	return w.sendCommandAsync(Publish(topic, body), doneChan, args)
}

// MultiPublishAsync publishes a slice of message bodies to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (w *Producer) MultiPublishAsync(topic string, body [][]byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return err
	}
	return w.sendCommandAsync(cmd, doneChan, args)
}

// Publish synchronously publishes a message body to the specified topic, returning
// an error if publish failed
func (w *Producer) Publish(topic string, body []byte) error {
	return w.sendCommand(Publish(topic, body))
}

// MultiPublish synchronously publishes a slice of message bodies to the specified topic, returning
// an error if publish failed
func (w *Producer) MultiPublish(topic string, body [][]byte) error {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return err
	}
	return w.sendCommand(cmd)
}

func (w *Producer) sendCommand(cmd *Command) error {
	doneChan := make(chan *ProducerTransaction)
	err := w.sendCommandAsync(cmd, doneChan, nil)
	if err != nil {
		close(doneChan)
		return err
	}
	t := <-doneChan
	return t.Error
}

func (w *Producer) sendCommandAsync(cmd *Command, doneChan chan *ProducerTransaction,
	args []interface{}) error {
	// keep track of how many outstanding producers we're dealing with
	// in order to later ensure that we clean them all up...
	atomic.AddInt32(&w.concurrentProducers, 1)
	defer atomic.AddInt32(&w.concurrentProducers, -1)

	if atomic.LoadInt32(&w.state) != StateConnected {
		err := w.connect()
		if err != nil {
			return err
		}
	}

	t := &ProducerTransaction{
		cmd:      cmd,
		doneChan: doneChan,
		Args:     args,
	}

	select {
	case w.transactionChan <- t:
	case <-w.exitChan:
		return ErrStopped
	}

	return nil
}

func (w *Producer) connect() error {
	w.guard.Lock()
	defer w.guard.Unlock()

	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return ErrStopped
	}

	switch state := atomic.LoadInt32(&w.state); state {
	case StateInit:
	case StateConnected:
		return nil
	default:
		return ErrNotConnected
	}

	w.log(LogLevelInfo, "(%s) connecting to nsqd", w.addr)

	logger, logLvl := w.getLogger()

	w.conn = NewConn(w.addr, &w.config, &producerConnDelegate{w})
	w.conn.SetLogger(logger, logLvl, fmt.Sprintf("%3d (%%s)", w.id))

	_, err := w.conn.Connect()
	if err != nil {
		w.conn.Close()
		w.log(LogLevelError, "(%s) error connecting to nsqd - %s", w.addr, err)
		return err
	}
	atomic.StoreInt32(&w.state, StateConnected)
	w.closeChan = make(chan int)
	w.wg.Add(1)
	go w.router()

	return nil
}

func (w *Producer) close() {
	if !atomic.CompareAndSwapInt32(&w.state, StateConnected, StateDisconnected) {
		return
	}
	w.conn.Close()
	go func() {
		// we need to handle this in a goroutine so we don't
		// block the caller from making progress
		w.wg.Wait()
		atomic.StoreInt32(&w.state, StateInit)
	}()
}

func (w *Producer) router() {
	for {
		select {
		case t := <-w.transactionChan:
			w.transactions = append(w.transactions, t)
			err := w.conn.WriteCommand(t.cmd)
			if err != nil {
				w.log(LogLevelError, "(%s) sending command - %s", w.conn.String(), err)
				w.close()
			}
		case data := <-w.responseChan:
			w.popTransaction(FrameTypeResponse, data)
		case data := <-w.errorChan:
			w.popTransaction(FrameTypeError, data)
		case <-w.closeChan:
			goto exit
		case <-w.exitChan:
			goto exit
		}
	}

exit:
	w.transactionCleanup()
	w.wg.Done()
	w.log(LogLevelInfo, "exiting router")
}

func (w *Producer) popTransaction(frameType int32, data []byte) {
	t := w.transactions[0]
	w.transactions = w.transactions[1:]
	if frameType == FrameTypeError {
		t.Error = ErrProtocol{string(data)}
		if IsFailedOnNotLeader(t.Error) || IsTopicNotExist(t.Error) {
			// TODO: notify to reload topic-producer relation.
		}
	}
	t.finish()
}

func (w *Producer) transactionCleanup() {
	// clean up transactions we can easily account for
	for _, t := range w.transactions {
		t.Error = ErrNotConnected
		t.finish()
	}
	w.transactions = w.transactions[:0]

	// spin and free up any writes that might have raced
	// with the cleanup process (blocked on writing
	// to transactionChan)
	for {
		select {
		case t := <-w.transactionChan:
			t.Error = ErrNotConnected
			t.finish()
		default:
			// keep spinning until there are 0 concurrent producers
			if atomic.LoadInt32(&w.concurrentProducers) == 0 {
				return
			}
			// give the runtime a chance to schedule other racing goroutines
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (w *Producer) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl := w.getLogger()

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(2, fmt.Sprintf("%-4s %3d %s", lvl, w.id, fmt.Sprintf(line, args...)))
}

func (w *Producer) onConnResponse(c *Conn, data []byte) { w.responseChan <- data }
func (w *Producer) onConnError(c *Conn, data []byte)    { w.errorChan <- data }
func (w *Producer) onConnHeartbeat(c *Conn)             {}
func (w *Producer) onConnIOError(c *Conn, err error)    { w.close() }
func (w *Producer) onConnClose(c *Conn) {
	w.guard.Lock()
	defer w.guard.Unlock()
	close(w.closeChan)
}

// the strategy how the message publish on different partitions
type PubStrategyType int

const (
	PubRR PubStrategyType = iota
	PubDynamicLoad
	PubIDHash
)

type TopicPartProducerInfo struct {
	currentIndex  int32
	allPartitions []string
}

func NewTopicPartProducerInfo(capacity int) *TopicPartProducerInfo {
	return &TopicPartProducerInfo{
		currentIndex:  0,
		allPartitions: make([]string, 0, capacity),
	}
}

func (self *TopicPartProducerInfo) updatePartitionInfo(pid int, addr string) {
	for len(self.allPartitions) <= pid {
		self.allPartitions = append(self.allPartitions, "")
	}
	self.allPartitions[pid] = addr
}

func (self *TopicPartProducerInfo) removePartitionInfo(pid int) string {
	if len(self.allPartitions) <= pid {
		return ""
	}
	removed := self.allPartitions[pid]
	self.allPartitions[pid] = ""
	return removed
}

func (self *TopicPartProducerInfo) getPartitionInfo(pid int) string {
	if len(self.allPartitions) <= pid {
		return ""
	}
	addr := self.allPartitions[pid]
	return addr
}

type TopicProducerMgr struct {
	pubStrategy        PubStrategyType
	producerMtx        sync.RWMutex
	topicMtx           sync.RWMutex
	topics             map[string]*TopicPartProducerInfo
	producers          map[string]*Producer
	config             Config
	etcdServers        []string
	etcdClusterID      string
	etcdLookupPath     string
	lookupdHTTPAddrs   []string
	logger             logger
	logLvl             LogLevel
	logGuard           sync.RWMutex
	mtx                sync.RWMutex
	lookupdQueryIndex  int
	exitChan           chan int
	wg                 sync.WaitGroup
	lookupdRecheckChan chan int
}

// use part=-1 to handle all partitions of topic
func NewTopicProducerMgr(topics []string, strategy PubStrategyType, conf *Config) (*TopicProducerMgr, error) {
	conf.assertInitialized()
	err := conf.Validate()
	if err != nil {
		return nil, err
	}

	mgr := &TopicProducerMgr{
		topics:             make(map[string]*TopicPartProducerInfo, len(topics)),
		pubStrategy:        strategy,
		producers:          make(map[string]*Producer),
		config:             *conf,
		lookupdRecheckChan: make(chan int, 1),
		exitChan:           make(chan int),
	}
	for _, t := range topics {
		mgr.topics[t] = NewTopicPartProducerInfo(0)
	}
	return mgr, nil
}

func (self *TopicProducerMgr) SetEtcdConf(servers []string, cluster string, lookupPath string) {
	self.etcdServers = servers
	self.etcdClusterID = cluster
	self.etcdLookupPath = lookupPath
}

func (self *TopicProducerMgr) AddLookupdNodes(addresses []string) {
	for _, addr := range addresses {
		self.ConnectToNSQLookupd(addr)
	}
}

func (self *TopicProducerMgr) ConnectToNSQLookupd(addr string) error {
	if err := validatedLookupAddr(addr); err != nil {
		return err
	}

	self.mtx.Lock()
	for _, x := range self.lookupdHTTPAddrs {
		if x == addr {
			self.mtx.Unlock()
			return nil
		}
	}
	self.lookupdHTTPAddrs = append(self.lookupdHTTPAddrs, addr)
	numLookupd := len(self.lookupdHTTPAddrs)
	self.mtx.Unlock()

	self.log(LogLevelInfo, "new lookupd address added: %s", addr)
	// if this is the first one, kick off the go loop
	if numLookupd == 1 {
		go self.lookupLoop()
	}
	return nil
}

func (self *TopicProducerMgr) nextLookupdEndpoint() (map[string]string, string) {
	self.mtx.RLock()
	if self.lookupdQueryIndex >= len(self.lookupdHTTPAddrs) {
		self.lookupdQueryIndex = 0
	}
	addr := self.lookupdHTTPAddrs[self.lookupdQueryIndex]
	num := len(self.lookupdHTTPAddrs)
	self.mtx.RUnlock()
	self.lookupdQueryIndex = (self.lookupdQueryIndex + 1) % num

	urlString := addr
	if !strings.Contains(urlString, "://") {
		urlString = "http://" + addr
	}

	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	listUrl := *u
	if u.Path == "/" || u.Path == "" {
		u.Path = "/lookup"
	}
	listUrl.Path = "/listlookup"

	urlList := make(map[string]string, 0)
	for t, _ := range self.topics {
		tmpUrl := *u
		v, _ := url.ParseQuery(tmpUrl.RawQuery)
		v.Add("topic", t)
		tmpUrl.RawQuery = v.Encode()
		urlList[t] = tmpUrl.String()
	}
	return urlList, listUrl.String()
}

func (self *TopicProducerMgr) queryLookupd() {
	topicQueryList, discoveryUrl := self.nextLookupdEndpoint()
	// discovery other lookupd nodes from current lookupd or from etcd
	self.log(LogLevelInfo, "discovery nsqlookupd %s", discoveryUrl)
	var lookupdList lookupListResp
	err := apiRequestNegotiateV1("GET", discoveryUrl, nil, &lookupdList)
	if err != nil {
		self.log(LogLevelError, "error discovery nsqlookupd (%s) - %s", discoveryUrl, err)
	} else {
		for _, addr := range lookupdList.LookupdNodes {
			self.ConnectToNSQLookupd(addr)
		}
	}

	for topicName, topicUrl := range topicQueryList {
		self.log(LogLevelInfo, "querying nsqlookupd for topic %s", topicUrl)
		self.topicMtx.Lock()
		partProducerInfo, ok := self.topics[topicName]
		self.topicMtx.Unlock()
		if !ok {
			self.log(LogLevelError, "topic %v is not set.", topicName)
			continue
		}
		var data lookupResp
		err = apiRequestNegotiateV1("GET", topicUrl, nil, &data)
		if err != nil {
			self.log(LogLevelError, "error querying nsqlookupd (%s) - %s", topicUrl, err)
			continue
		}

		newProducerInfo := NewTopicPartProducerInfo(len(data.Partitions))
		changed := false
		for partStr, producer := range data.Partitions {
			partID, err := strconv.Atoi(partStr)
			if err != nil {
				self.log(LogLevelError, "got partition producer with invalid partition string: %v", partStr)
				continue
			}
			broadcastAddress := producer.BroadcastAddress
			port := producer.TCPPort
			addr := net.JoinHostPort(broadcastAddress, strconv.Itoa(port))
			newProducerInfo.updatePartitionInfo(partID, addr)
			oldAddr := partProducerInfo.getPartitionInfo(partID)
			if oldAddr != addr {
				self.log(LogLevelInfo, "topic %v partition [%v] producer changed from [%v] to [%v]", topicName, partID, oldAddr, addr)
				changed = true
			}
		}
		if !changed {
			continue
		}

		self.producerMtx.Lock()
		for partID, addr := range newProducerInfo.allPartitions {
			_, ok := self.producers[addr]
			if !ok {
				self.log(LogLevelInfo, "init new producer %v for topic %v-%v", addr, topicName, partID)
				newProd, err := NewProducer(addr, &self.config)
				if err != nil {
					self.log(LogLevelError, "producer %v init failed: %v", addr, err)
				} else {
					newProd.SetLogger(self.getLogger())
					self.producers[addr] = newProd
				}
			}
		}
		self.producerMtx.Unlock()

		self.topicMtx.Lock()
		self.topics[topicName] = newProducerInfo
		self.topicMtx.Unlock()
	}
}

func (self *TopicProducerMgr) lookupLoop() {
	ticker := time.NewTicker(self.config.LookupdPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			self.queryLookupd()
		case <-self.lookupdRecheckChan:
			self.queryLookupd()
		case <-self.exitChan:
			return
		}
	}
}

func (self *TopicProducerMgr) getNextProducerAddr(partProducerInfo *TopicPartProducerInfo) (int, string) {
	addr := ""
	length := len(partProducerInfo.allPartitions)
	if length == 0 {
		return -1, addr
	} else if length == 1 {
		return 0, partProducerInfo.getPartitionInfo(0)
	}
	retry := 0
	pid := -1
	for addr == "" {
		if retry >= length {
			break
		}
		if self.pubStrategy == PubRR {
			if partProducerInfo.currentIndex >= int32(len(partProducerInfo.allPartitions))-1 {
				partProducerInfo.currentIndex = 0
			}
			pid = int(atomic.AddInt32(&partProducerInfo.currentIndex, 1))
			addr = partProducerInfo.getPartitionInfo(pid)
			retry++
		} else {
			// TODO: other pub strategy
			break
		}
	}
	return pid, addr
}

func (self *TopicProducerMgr) getProducer(topic string) (*Producer, int, error) {
	self.topicMtx.RLock()
	partProducerInfo, ok := self.topics[topic]
	self.topicMtx.RUnlock()
	if !ok {
		return nil, -1, ErrTopicNotSet
	}
	pid, addr := self.getNextProducerAddr(partProducerInfo)
	if addr == "" {
		select {
		case self.lookupdRecheckChan <- 1:
		default:
		}
		return nil, pid, ErrNoProducer
	}
	self.producerMtx.RLock()
	producer, ok := self.producers[addr]
	self.producerMtx.RUnlock()
	if !ok {
		return nil, pid, ErrNoProducer
	}
	return producer, pid, nil
}

func (self *TopicProducerMgr) PublishAsync(topic string, body []byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	retry := 0
	var err error
	var producer *Producer
	pid := -1
	for retry < 3 {
		retry++
		producer, pid, err = self.getProducer(topic)
		if err != nil {
			if err == ErrNoProducer {
				time.Sleep(time.Millisecond * time.Duration(10*retry))
				continue
			} else {
				break
			}
		}
		err = producer.sendCommandAsync(PublishWithPart(topic, strconv.Itoa(pid), body), doneChan, args)
		if err != nil {
			time.Sleep(time.Millisecond * time.Duration(10*retry))
		} else {
			break
		}
	}
	return err
}

func (self *TopicProducerMgr) MultiPublishAsync(topic string, body [][]byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	producer, pid, err := self.getProducer(topic)
	if err != nil {
		return err
	}

	cmd, err := MultiPublishWithPart(topic, strconv.Itoa(pid), body)
	if err != nil {
		return err
	}
	return producer.sendCommandAsync(cmd, doneChan, args)
}

func (self *TopicProducerMgr) Publish(topic string, body []byte) error {
	retry := 0
	var err error
	var producer *Producer
	pid := -1
	for retry < 3 {
		retry++
		producer, pid, err = self.getProducer(topic)
		if err != nil {
			if err == ErrNoProducer {
				time.Sleep(time.Millisecond * time.Duration(10*retry))
				continue
			} else {
				break
			}
		}
		err = producer.sendCommand(PublishWithPart(topic, strconv.Itoa(pid), body))
		if err != nil {
			if IsFailedOnNotLeader(err) || IsTopicNotExist(err) {
				select {
				case self.lookupdRecheckChan <- 1:
				default:
				}
				time.Sleep(time.Millisecond * 100)
			}
			time.Sleep(time.Millisecond * time.Duration(10*retry))
		} else {
			break
		}
	}
	return err
}

func (self *TopicProducerMgr) MultiPublish(topic string, body [][]byte) error {
	producer, pid, err := self.getProducer(topic)
	if err != nil {
		return err
	}
	cmd, err := MultiPublishWithPart(topic, strconv.Itoa(pid), body)
	if err != nil {
		return err
	}

	return producer.sendCommand(cmd)
}

func (self *TopicProducerMgr) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl := self.getLogger()

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(2, fmt.Sprintf("%-4s %3d %s", lvl, fmt.Sprintf(line, args...)))
}

func (self *TopicProducerMgr) SetLogger(l logger, lvl LogLevel) {
	self.logGuard.Lock()
	defer self.logGuard.Unlock()

	self.logger = l
	self.logLvl = lvl
}

func (self *TopicProducerMgr) getLogger() (logger, LogLevel) {
	self.logGuard.RLock()
	defer self.logGuard.RUnlock()
	return self.logger, self.logLvl
}
