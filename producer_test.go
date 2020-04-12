package nsq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/assert"
)

type ConsumerHandler struct {
	t              *testing.T
	q              *Consumer
	messagesGood   int
	messagesFailed int
	Msgs           []*Message
	checkResult    map[string]bool
}

func (h *ConsumerHandler) LogFailedMessage(message *Message) {
	msg := string(message.Body)
	h.messagesFailed++
	h.t.Logf("got failed message :%v ", msg)
	h.q.Stop()
}

func (h *ConsumerHandler) HandleMessage(message *Message) error {
	msg := string(message.Body)
	if message.Attempts == 1 {
		h.Msgs = append(h.Msgs, message)
	}
	if msg == "bad_test_case" {
		return errors.New("fail this message")
	}
	if msg != "multipublish_test_case" && msg != "publish_test_case" && !strings.Contains(msg, "lookupd_test_case") {
		h.t.Error("message 'action' was not correct:", msg)
	}
	if strings.Contains(msg, "lookupd_test_case") {
		result, ok := h.checkResult[msg]
		if ok && result == true {
			h.t.Error("message 'action' was duplicated:", msg)
		}
		h.checkResult[msg] = true
	}
	h.t.Logf("got message :%v ", msg)
	h.messagesGood++
	return nil
}

func ensureInitChannelExt(t *testing.T, topicName string, useLookup bool) {
	config := NewConfig()
	config.DefaultRequeueDelay = 0
	config.MaxBackoffDuration = 50 * time.Millisecond
	q, _ := NewConsumer(topicName, "ch", config)
	q.SetLogger(nullLogger, LogLevelInfo)

	h := &ConsumerHandler{
		t: t,
		q: q,
	}
	q.AddHandler(h)
	q.SetConsumeExt(true)

	if useLookup {
		err := q.ConnectToNSQLookupd("127.0.0.1:4161")
		if err != nil {
			t.Fatalf(err.Error())
		}
	} else {
		err := q.ConnectToNSQD("127.0.0.1:4150", 0)
		if err != nil {
			t.Errorf("init error: %v", err.Error())
		}
	}
	q.Stop()
}

func ensureInitChannel(t *testing.T, topicName string, useLookup bool) {
	config := NewConfig()
	config.DefaultRequeueDelay = 0
	config.MaxBackoffDuration = 50 * time.Millisecond
	q, _ := NewConsumer(topicName, "ch", config)
	q.SetLogger(nullLogger, LogLevelInfo)

	h := &ConsumerHandler{
		t: t,
		q: q,
	}
	q.AddHandler(h)
	q.backoff(time.Second * 3)
	if useLookup {
		err := q.ConnectToNSQLookupd("127.0.0.1:4161")
		if err != nil {
			t.Fatalf(err.Error())
		}
	} else {
		err := q.ConnectToNSQD("127.0.0.1:4150", 0)
		if err != nil {
			t.Errorf("init error: %v", err.Error())
		}
	}

	time.Sleep(time.Second)
	q.Stop()
}

func TestProducerConnection(t *testing.T) {
	config := NewConfig()
	laddr := "127.0.0.1"

	config.LocalAddr, _ = net.ResolveTCPAddr("tcp", laddr+":0")

	EnsureTopic(t, 4150, "write_test", 0)
	ensureInitChannel(t, "write_test", false)
	w, _ := NewProducer("127.0.0.1:4150", config)
	w.SetLogger(nullLogger, LogLevelInfo)

	err := w.Publish("write_test", []byte("test"))
	if err != nil {
		t.Fatalf("should lazily connect - %s", err)
	}

	w.Stop()

	err = w.Publish("write_test", []byte("fail test"))
	if err != ErrStopped {
		t.Fatalf("should not be able to write after Stop()")
	}
}

func TestProducerPing(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	config := NewConfig()
	w, _ := NewProducer("127.0.0.1:4150", config)
	w.SetLogger(nullLogger, LogLevelInfo)

	err := w.Ping()

	if err != nil {
		t.Fatalf("should connect on ping")
	}

	w.Stop()

	err = w.Ping()
	if err != ErrStopped {
		t.Fatalf("should not be able to ping after Stop()")
	}
}

func TestProducerPublish(t *testing.T) {
	topicName := "publish" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10
	EnsureTopic(t, 4150, topicName, 0)
	ensureInitChannel(t, topicName, false)

	config := NewConfig()
	w, _ := NewProducer("127.0.0.1:4150", config)
	w.SetLogger(nullLogger, LogLevelInfo)
	defer w.Stop()

	for i := 0; i < msgCount; i++ {
		err := w.Publish(topicName, []byte("publish_test_case"))
		if err != nil {
			t.Fatalf("error %s", err)
		}
	}

	err := w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("error %s", err)
	}

	readMessages(topicName, t, msgCount, false)
}

func TestProducerMultiPublish(t *testing.T) {
	topicName := "multi_publish" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10

	EnsureTopic(t, 4150, topicName, 0)
	ensureInitChannel(t, topicName, false)

	config := NewConfig()
	w, _ := NewProducer("127.0.0.1:4150", config)
	w.SetLogger(nullLogger, LogLevelInfo)
	defer w.Stop()

	var testData [][]byte
	for i := 0; i < msgCount; i++ {
		testData = append(testData, []byte("multipublish_test_case"))
	}

	err := w.MultiPublish(topicName, testData)
	if err != nil {
		t.Fatalf("error %s", err)
	}

	err = w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("error %s", err)
	}

	readMessages(topicName, t, msgCount, false)
}

func TestProducerPublishAsync(t *testing.T) {
	topicName := "async_publish" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10

	EnsureTopic(t, 4150, topicName, 0)
	ensureInitChannel(t, topicName, false)

	config := NewConfig()
	w, _ := NewProducer("127.0.0.1:4150", config)
	w.SetLogger(nullLogger, LogLevelInfo)
	defer w.Stop()

	responseChan := make(chan *ProducerTransaction, msgCount)
	for i := 0; i < msgCount; i++ {
		err := w.PublishAsync(topicName, []byte("publish_test_case"), responseChan, "test")
		if err != nil {
			t.Fatalf(err.Error())
		}
	}

	for i := 0; i < msgCount; i++ {
		trans := <-responseChan
		if trans.Error != nil {
			t.Fatalf(trans.Error.Error())
		}
		if trans.Args[0].(string) != "test" {
			t.Fatalf(`proxied arg "%s" != "test"`, trans.Args[0].(string))
		}
	}

	err := w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("error %s", err)
	}

	readMessages(topicName, t, msgCount, false)
}

func TestProducerMultiPublishAsync(t *testing.T) {
	topicName := "multi_publish" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10

	EnsureTopic(t, 4150, topicName, 0)
	ensureInitChannel(t, topicName, false)

	config := NewConfig()
	w, _ := NewProducer("127.0.0.1:4150", config)
	w.SetLogger(nullLogger, LogLevelInfo)
	defer w.Stop()

	var testData [][]byte
	for i := 0; i < msgCount; i++ {
		testData = append(testData, []byte("multipublish_test_case"))
	}

	responseChan := make(chan *ProducerTransaction, 1)
	err := w.MultiPublishAsync(topicName, testData, responseChan, "test0", 1)
	if err != nil {
		t.Fatalf(err.Error())
	}

	trans := <-responseChan
	if trans.Error != nil {
		t.Fatalf(trans.Error.Error())
	}
	if trans.Args[0].(string) != "test0" {
		t.Fatalf(`proxied arg "%s" != "test0"`, trans.Args[0].(string))
	}
	if trans.Args[1].(int) != 1 {
		t.Fatalf(`proxied arg %d != 1`, trans.Args[1].(int))
	}

	err = w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("error %s", err)
	}

	readMessages(topicName, t, msgCount, false)
}

func TestProducerPublishTrace(t *testing.T) {
	topicName := "publish_trace" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10
	EnsureTopic(t, 4150, topicName, 0)
	ensureInitChannel(t, topicName, false)

	config := NewConfig()
	w, _ := NewProducer("127.0.0.1:4150", config)
	w.SetLogger(nullLogger, LogLevelInfo)
	defer w.Stop()

	for i := 0; i < msgCount; i++ {
		cmd, _ := PublishTrace(topicName, "0", uint64(i+1), []byte("publish_test_case"))
		resp, err := w.sendCommand(cmd)
		if err != nil {
			t.Fatalf("error %s", err)
		}
		if len(resp) != 2+MsgIDLength+8+4 {
			t.Fatalf("invalid trace response: %v", resp)
		}
	}

	err := w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("error %s", err)
	}

	readMessages(topicName, t, msgCount, false)
}

func TestProducerPublishWithExt(t *testing.T) {
	topicName := "publish_ext" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10
	EnsureTopicWithExt(t, 4150, topicName, 0, true)
	ensureInitChannel(t, topicName, false)

	config := NewConfig()
	w, _ := NewProducer("127.0.0.1:4150", config)
	w.SetLogger(nullLogger, LogLevelInfo)
	defer w.Stop()

	for i := 0; i < msgCount; i++ {
		var e MsgExt
		if i > msgCount/2 {
			e.TraceID = uint64(i)
		}
		cmd, _ := PublishWithJsonExt(topicName, "0", []byte("publish_test_case"), e.ToJson())
		resp, err := w.sendCommand(cmd)
		if err != nil {
			t.Fatalf("error %s", err)
		}
		if i > msgCount/2 {
			if len(resp) != 2+MsgIDLength+8+4 {
				t.Errorf("invalid trace response: %v, %v", resp, string(e.ToJson()))
			}
		} else {
			if len(resp) != 2 {
				t.Fatalf("invalid trace response: %v", resp)
			}
		}
	}

	err := w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("error %s", err)
	}

	readExtMessages(topicName, t, msgCount, false)
}

func TestProducerHeartbeat(t *testing.T) {
	topicName := "heartbeat" + strconv.Itoa(int(time.Now().Unix()))

	EnsureTopic(t, 4150, topicName, 0)
	ensureInitChannel(t, topicName, false)

	config := NewConfig()
	config.HeartbeatInterval = 100 * time.Millisecond
	w, _ := NewProducer("127.0.0.1:4150", config)
	w.SetLogger(nullLogger, LogLevelInfo)
	defer w.Stop()

	err := w.Publish(topicName, []byte("publish_test_case"))
	if err == nil {
		t.Fatalf("error should not be nil")
	}
	if identifyError, ok := err.(ErrIdentify); !ok ||
		identifyError.Reason != "E_BAD_BODY IDENTIFY heartbeat interval (100) is invalid" {
		t.Fatalf("wrong error - %s", err)
	}

	config = NewConfig()
	config.HeartbeatInterval = 1000 * time.Millisecond
	w, _ = NewProducer("127.0.0.1:4150", config)
	w.SetLogger(nullLogger, LogLevelInfo)
	defer w.Stop()

	err = w.Publish(topicName, []byte("publish_test_case"))
	if err != nil {
		t.Fatalf(err.Error())
	}

	time.Sleep(1100 * time.Millisecond)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		err := w.Publish(topicName, []byte("publish_test_case"))
		if err != nil {
			t.Fatalf("error %s", err)
		}
	}

	err = w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("error %s", err)
	}

	readMessages(topicName, t, msgCount+1, false)
}

func TestProducerPublishWithTimeout(t *testing.T) {
	topicName := "publish_timeout" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 3
	//EnsureTopicWithExt(t, 4150, topicName, 0, true)
	//ensureInitChannel(t, topicName, false)

	config := NewConfig()
	config.PubTimeout = time.Second
	w, _ := NewProducer("127.0.0.1:4150", config)
	w.SetLogger(nullLogger, LogLevelInfo)

	w.conn = newMockProducerConn(&producerConnDelegate{w})
	atomic.StoreInt32(&w.state, StateConnected)
	// do not run router for producer, so we can test timeout

	defer w.Stop()

	for i := 0; i < msgCount; i++ {
		cmd := Publish(topicName, []byte("publish_test_case"))
		_, err := w.sendCommand(cmd)
		if err != context.DeadlineExceeded {
			t.Fatalf("error %s", err)
		}
	}
	err := w.Publish(topicName, []byte("bad_test_case"))
	if err != context.DeadlineExceeded {
		t.Fatalf("error %s", err)
	}
}

func TestProducerPublishToNotLeader(t *testing.T) {
	// TODO:
}

func TestTopicProducerMgrDynamicTopic(t *testing.T) {
	testTopicProducerMgr(t, true)
}

func TestTopicProducerMgrStaticTopic(t *testing.T) {
	testTopicProducerMgr(t, false)
}

func TestTopicProducerMgrGetNextProducer(t *testing.T) {
	topicName := "topic_producer_mgr_publish" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10
	topicList := make([]string, 0)
	for i := 0; i < 3; i++ {
		topicList = append(topicList, "t"+strconv.Itoa(i)+topicName)
		EnsureTopic(t, 4150, "t"+strconv.Itoa(i)+topicName, 0)
		EnsureTopic(t, 4150, "t"+strconv.Itoa(i)+topicName, 1)
		EnsureTopic(t, 4150, "t"+strconv.Itoa(i)+topicName, 2)
	}

	// wait nsqd report to lookupd
	time.Sleep(time.Second * 3)
	for i := 0; i < 3; i++ {
		ensureInitChannel(t, "t"+strconv.Itoa(i)+topicName, true)
	}

	config := NewConfig()
	initTopics := make([]string, 0)
	initTopics = topicList
	config.PubStrategy = PubRR
	w, err := NewTopicProducerMgr(initTopics, config)
	if err != nil {
		t.Fatal(err)
	}
	w.SetLogger(newTestLogger(t), LogLevelInfo)
	lookupList := make([]string, 0)
	lookupList = append(lookupList, "127.0.0.1:4161")
	w.AddLookupdNodes(lookupList)
	defer w.Stop()

	var testData [][]byte
	for i := 0; i < msgCount; i++ {
		testData = append(testData, []byte("multipublish_test_case"))
	}

	for _, tn := range topicList {
		_, pid, err := w.getProducer(tn, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, pid2, err := w.getProducer(tn, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, pid3, err := w.getProducer(tn, nil)
		if err != nil {
			t.Fatal(err)
		}
		if pid == pid2 || pid == pid3 || pid2 == pid3 {
			t.Fatalf("should get different partitions for producer: %v, %v , %v", pid, pid2, pid3)
		}
	}
}

func TestTopicProducerMgrPubBackground(t *testing.T) {
	topicName := "topic_producer_mgr_pub_background" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 5
	EnsureTopic(t, 4150, topicName, 0)

	testingTimeout = true
	testingSendTimeout = true
	defer func() {
		testingSendTimeout = false
		testingTimeout = false
	}()
	time.Sleep(time.Second)

	config := NewConfig()
	config.PubTimeout = time.Second
	config.PubMaxBackgroundRetry = 10
	config.PubStrategy = PubRR
	w, err := NewTopicProducerMgr([]string{topicName}, config)
	if err != nil {
		t.Fatal(err)
	}
	w.SetLogger(newTestLogger(t), LogLevelDebug)
	lookupList := make([]string, 0)
	lookupList = append(lookupList, "127.0.0.1:4161")
	w.AddLookupdNodes(lookupList)
	defer w.Stop()

	for i := 0; i < msgCount; i++ {
		err = w.PublishAndRetryBackground(topicName, []byte("publish_test_case"))
		if err != ErrRetryBackground {
			t.Error(err)
		}
	}
	time.Sleep(time.Second)
	ensureInitChannel(t, topicName, false)
	// wait test send timeout
	time.Sleep(config.PubTimeout * 4)
	testingSendTimeout = false
	// test send timeout done
	// wait test read response timeout
	time.Sleep(time.Second * 20)
	testingTimeout = false
	time.Sleep(2 * time.Second)

	err = w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("error %s", err)
	}

	readMessages2(topicName, t, msgCount, true, true)
	time.Sleep(time.Second * 5)
}

func TestTopicProducerMgrPubOrdered(t *testing.T) {
	stopC := make(chan struct{})
	var meta metaInfo
	meta.PartitionNum = 1
	meta.Replica = 1
	topicName := "topic_producer_mgr_pub_order" + strconv.Itoa(int(time.Now().Unix()))
	topicList := make([]string, 0)
	topicList = append(topicList, topicName)
	ensureFakedLookup(t, "127.0.0.1:4165", meta, topicList, stopC)
	defer func() {
		close(stopC)
		time.Sleep(time.Second)
	}()

	msgCount := 5
	EnsureTopic(t, 4150, topicName, 0)
	ensureInitChannel(t, topicName, false)

	time.Sleep(time.Second)

	config := NewConfig()
	config.PubTimeout = time.Second
	config.PubMaxBackgroundRetry = 10
	config.EnableOrdered = true
	config.EnableTrace = true
	config.Hasher = murmur3.New32()
	w, err := NewTopicProducerMgr([]string{topicName}, config)
	if err != nil {
		t.Fatal(err)
	}
	w.SetLogger(newTestLogger(t), LogLevelInfo)
	lookupList := make([]string, 0)
	lookupList = append(lookupList, "127.0.0.1:4165")
	w.AddLookupdNodes(lookupList)
	defer w.Stop()

	for i := 0; i < msgCount; i++ {
		id, offset, size, err := w.PublishOrdered(topicName, []byte("publish_test_case"), []byte("publish_test_case"))
		if err != nil {
			t.Error(err)
		}
		t.Logf("pubto %v, %v, %v", id, offset, size)
	}
	time.Sleep(time.Second)

	err = w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("error %s", err)
	}

	readMessages2(topicName, t, msgCount, true, true)
}

func TestTopicProducerMgrPubShouldFailed(t *testing.T) {
	stopC := make(chan struct{})
	var meta metaInfo
	meta.PartitionNum = 1
	meta.Replica = 1
	topicName := "topic_producer_mgr_pub_fail" + strconv.Itoa(int(time.Now().Unix()))
	topicList := make([]string, 0)
	topicList = append(topicList, topicName)
	ensureFakedLookup(t, "127.0.0.1:4165", meta, topicList, stopC)
	defer func() {
		close(stopC)
		time.Sleep(time.Second)
	}()

	msgCount := 50

	config := NewConfig()
	config.PubTimeout = time.Second
	w, err := NewTopicProducerMgr([]string{topicName}, config)
	if err != nil {
		t.Fatal(err)
	}
	w.SetLogger(newTestLogger(t), LogLevelDebug)
	lookupList := make([]string, 0)
	lookupList = append(lookupList, "127.0.0.1:4165")
	w.AddLookupdNodes(lookupList)
	defer w.Stop()

	for i := 0; i < msgCount; i++ {
		err := w.Publish(topicName, []byte("publish_test_case"))
		if err == nil {
			t.Error("should return error since pub failed")
			break
		}
	}
}

func TestTopicProducerMgrRemoveFailedLookupd(t *testing.T) {
	topicName := "topic_producer_mgr_failed_lookup" + strconv.Itoa(int(time.Now().Unix()))
	topicList := make([]string, 0)
	for i := 0; i < 1; i++ {
		topicList = append(topicList, "t"+strconv.Itoa(i)+topicName)
		EnsureTopic(t, 4150, "t"+strconv.Itoa(i)+topicName, 0)
	}

	// wait nsqd report to lookupd
	time.Sleep(time.Second * 3)
	for i := 0; i < 1; i++ {
		ensureInitChannel(t, "t"+strconv.Itoa(i)+topicName, true)
	}

	config := NewConfig()
	config.LookupdSeeds = append(config.LookupdSeeds, "127.0.0.1:4161")
	// add failed lookupd to seeds
	config.LookupdSeeds = append(config.LookupdSeeds, "127.0.0.1:5161")
	config.LookupdPollInterval = time.Second
	initTopics := make([]string, 0)
	initTopics = topicList
	config.PubStrategy = PubRR
	w, err := NewTopicProducerMgr(initTopics, config)
	if err != nil {
		t.Fatal(err)
	}

	w.ConnectToSeeds()
	// add failed lookupd by force
	w.mtx.Lock()
	w.lookupdHTTPAddrs = append(w.lookupdHTTPAddrs, "127.0.0.1:6161")
	w.mtx.Unlock()
	w.SetLogger(newTestLogger(t), LogLevelInfo)
	defer w.Stop()

	for _, tn := range topicList {
		_, _, err := w.getProducer(tn, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	cnt := 60
	passCnt := 0
	for cnt > 0 {
		time.Sleep(time.Second)
		cnt--
		ret := func() bool {
			w.mtx.Lock()
			defer w.mtx.Unlock()
			if FindString(w.lookupdHTTPAddrs, "127.0.0.1:6161") != -1 {
				t.Logf("should remove failed lookupd: %v", w.lookupdHTTPAddrs)
				return false
			}
			if FindString(w.lookupdHTTPAddrs, "127.0.0.1:5161") == -1 {
				t.Logf("should keep seed for failed lookupd: %v", w.lookupdHTTPAddrs)
				return false
			}
			if FindString(w.lookupdHTTPAddrs, "127.0.0.1:4161") == -1 {
				t.Logf("should keep normal lookupd: %v", w.lookupdHTTPAddrs)
				return false
			}
			passCnt++
			t.Logf("lookupd: %v", w.lookupdHTTPAddrs)
			if passCnt > 3 {
				return true
			}
			return false
		}()
		if ret {
			break
		}
	}
	if cnt <= 0 || passCnt <= 0 {
		t.Errorf("test failed")
	}
}

func TestTopicProducerMgrRemoveNsqdNode(t *testing.T) {
	// cases:
	// 1. one topic, remove a node and test if removed
	// 2. readd removed, test if added
	// 3. two topic, one topic remove a node but another keep this node, test should keep
	// 4. two topic, both topics removed the same node, test should remove this node
	oldKeep := removingKeepTime
	removingKeepTime = time.Second * 10
	defer func() {
		removingKeepTime = oldKeep
	}()
	stopC := make(chan struct{})
	var meta metaInfo
	meta.PartitionNum = 2
	meta.Replica = 2
	topicName := "test_remove_node" + strconv.Itoa(int(time.Now().Unix()))
	topicList := make([]string, 0)
	for i := 0; i < 2; i++ {
		topicList = append(topicList, "t"+strconv.Itoa(i)+topicName)
	}
	fakedLookup, allPeers, usedPeers := ensureFakedLookup(t, "127.0.0.1:4165", meta, topicList, stopC)
	defer func() {
		close(stopC)
		time.Sleep(time.Second)
	}()

	config := NewConfig()
	config.LookupdSeeds = append(config.LookupdSeeds, "127.0.0.1:4165")
	config.LookupdPollInterval = time.Second
	initTopics := make([]string, 0)
	initTopics = topicList
	config.PubStrategy = PubRR
	w, err := NewTopicProducerMgr(initTopics, config)
	if err != nil {
		t.Fatal(err)
	}
	w.SetLogger(newTestLogger(t), LogLevelInfo)
	w.ConnectToSeeds()
	for _, tname := range topicList {
		w.topicMtx.Lock()
		parts, ok := w.topics[tname]
		if !ok {
			t.Errorf("topic %v producers not found", tname)
			continue
		}
		for _, p := range parts.allPartitions {
			_, ok := usedPeers[p.addr]
			if !ok {
				t.Errorf("unused peer in producer manager: %v", p)
			}
		}
		w.topicMtx.Unlock()
		w.producerMtx.Lock()
		for addr, _ := range w.producers {
			_, ok := usedPeers[addr]
			if !ok {
				t.Errorf("unused peer in producer manager: %v", addr)
			}
		}
		if len(w.removingProducers) != 0 {
			t.Errorf("removing nodes should be 0")
		}
		w.producerMtx.Unlock()
	}
	// change producer for partition
	t0Resp := fakedLookup.fakeResponse[topicList[0]]
	oldPeer := t0Resp.Partitions["0"]
	oldAddr := net.JoinHostPort(oldPeer.BroadcastAddress, strconv.Itoa(oldPeer.TCPPort))
	t0Resp.Partitions["0"] = allPeers[meta.PartitionNum]
	newPeer := t0Resp.Partitions["0"]
	newAddr := net.JoinHostPort(newPeer.BroadcastAddress, strconv.Itoa(newPeer.TCPPort))
	usedPeers[newAddr] = newPeer
	time.Sleep(config.LookupdPollInterval * 2)
	w.topicMtx.Lock()
	parts, ok := w.topics[topicList[0]]
	if !ok {
		t.Errorf("topic %v producers not found", topicList[0])
	}
	for _, p := range parts.allPartitions {
		_, ok := usedPeers[p.addr]
		if !ok {
			t.Errorf("unused peer in producer manager: %v", p)
		}
	}
	w.topicMtx.Unlock()
	w.producerMtx.Lock()
	if len(w.producers) != len(usedPeers) {
		t.Error("producer number not match used")
	}
	for addr, _ := range w.producers {
		_, ok := usedPeers[addr]
		if !ok {
			t.Errorf("unused peer in producer manager: %v", addr)
		}
	}
	_, ok = w.removingProducers[oldAddr]
	if ok {
		t.Errorf("old node should not be in removing nodes since another topic is used")
	}
	w.producerMtx.Unlock()
	t0Resp = fakedLookup.fakeResponse[topicList[1]]
	oldPeer = t0Resp.Partitions["0"]
	oldAddr = net.JoinHostPort(oldPeer.BroadcastAddress, strconv.Itoa(oldPeer.TCPPort))
	// both topic removed this node
	delete(usedPeers, oldAddr)
	t0Resp.Partitions["0"] = allPeers[meta.PartitionNum]
	newPeer = t0Resp.Partitions["0"]
	newAddr = net.JoinHostPort(newPeer.BroadcastAddress, strconv.Itoa(newPeer.TCPPort))
	usedPeers[newAddr] = newPeer
	time.Sleep(config.LookupdPollInterval * 2)

	w.topicMtx.Lock()
	parts, ok = w.topics[topicList[0]]
	if !ok {
		t.Errorf("topic %v producers not found", topicList[0])
	}
	for _, p := range parts.allPartitions {
		_, ok := usedPeers[p.addr]
		if !ok {
			t.Errorf("unused peer in producer manager: %v", p)
		}
	}
	w.topicMtx.Unlock()
	w.producerMtx.Lock()

	if len(w.producers) != len(usedPeers) {
		t.Error("producer number not match used")
	}

	for addr, _ := range w.producers {
		_, ok := usedPeers[addr]
		if !ok {
			t.Errorf("unused peer in producer manager: %v", addr)
		}
	}
	_, ok = w.removingProducers[oldAddr]
	if !ok {
		t.Errorf("old node should be in removing nodes since both topic removed")
	}
	w.producerMtx.Unlock()
	time.Sleep(removingKeepTime)

	w.producerMtx.Lock()
	for addr, _ := range w.producers {
		if addr == oldAddr {
			t.Errorf("unused peer in producer manager: %v", addr)
		}
	}
	_, ok = w.removingProducers[oldAddr]
	if ok {
		t.Errorf("old node should be cleaned")
	}
	w.producerMtx.Unlock()
	// readd
	usedPeers = make(map[string]*peerInfo)
	for _, t := range topicList {
		rsp := lookupResp{
			Meta:       meta,
			Partitions: make(map[string]*peerInfo),
		}
		for i := 0; i < meta.PartitionNum; i++ {
			peer := allPeers[i]
			rsp.Producers = append(rsp.Producers, peer)
			rsp.Partitions[strconv.Itoa(i)] = peer
			usedPeers[net.JoinHostPort(peer.BroadcastAddress, strconv.Itoa(peer.TCPPort))] = peer
		}
		fakedLookup.fakeResponse[t] = rsp
	}
	time.Sleep(config.LookupdPollInterval * 2)
	w.topicMtx.Lock()
	parts, ok = w.topics[topicList[0]]
	if !ok {
		t.Errorf("topic %v producers not found", topicList[0])
	}
	if len(parts.allPartitions) != meta.PartitionNum {
		t.Error("part info length not match partition number")
	}
	for _, p := range parts.allPartitions {
		_, ok := usedPeers[p.addr]
		if !ok {
			t.Errorf("unused peer in producer manager: %v", p)
		}
	}
	w.topicMtx.Unlock()
	w.producerMtx.Lock()
	if len(w.producers) != len(usedPeers) {
		t.Error("producer number not match used")
	}
	for addr, _ := range w.producers {
		_, ok := usedPeers[addr]
		if !ok {
			t.Errorf("unused peer in producer manager: %v", addr)
		}
	}
	if len(w.removingProducers) == 0 {
		t.Errorf("removing should not be empty")
	}
	w.producerMtx.Unlock()
	time.Sleep(removingKeepTime)
	time.Sleep(config.LookupdPollInterval)
	w.producerMtx.Lock()
	if len(w.producers) != len(usedPeers) {
		t.Error("producer number not match used")
	}
	for addr, _ := range w.producers {
		_, ok := usedPeers[addr]
		if !ok {
			t.Errorf("unused peer in producer manager: %v", addr)
		}
	}
	if len(w.removingProducers) != 0 {
		t.Errorf("removing should be empty")
	}
	w.producerMtx.Unlock()
}

func TestTopicProducerMgrMultiPublishWithJsonExt(t *testing.T) {
	topicName := "topic_producer_mgr_mult_publish_ext"
	msgCount := 10
	extList := make([]*MsgExt, 0)
	msgs := make([][]byte, 0)
	//construct message
	for idx := 0; idx < msgCount; idx++ {
		var ext *MsgExt
		if idx%2 == 0 {
			ext = &MsgExt{}
		} else {
			ext = &MsgExt{
				TraceID:     12345,
				DispatchTag: "tag123",
				Custom:      map[string]interface{}{"key1": "val1", "key2": "val2"},
			}
		}
		extList = append(extList, ext)
		msgs = append(msgs, []byte("multipublish_test_case"))
	}

	badMsgCount := 1
	badExtList := make([]*MsgExt, 0)
	badMsgs := make([][]byte, 0)

	//construct message
	for idx := 0; idx < badMsgCount; idx++ {
		var ext *MsgExt
		if idx%2 == 0 {
			ext = &MsgExt{}
		} else {
			ext = &MsgExt{
				TraceID:     12345,
				DispatchTag: "tag123",
				Custom:      map[string]interface{}{"key1": "val1", "key2": "val2"},
			}
		}
		badExtList = append(badExtList, ext)
		badMsgs = append(badMsgs, []byte("bad_test_case"))
	}
	EnsureTopicWithExt(t, 4150, topicName, 0, true)
	ensureInitChannelExt(t, topicName, false)

	// wait nsqd report to lookupd
	time.Sleep(time.Second * 3)

	config := NewConfig()

	config.PubStrategy = PubRR
	config.DialTimeout = time.Second
	config.ReadTimeout = time.Second * 6
	config.HeartbeatInterval = time.Second * 3
	w, err := NewTopicProducerMgr([]string{topicName}, config)
	if err != nil {
		t.Fatal(err)
	}
	w.SetLogger(newTestLogger(t), LogLevelInfo)
	lookupList := make([]string, 0)
	lookupList = append(lookupList, "127.0.0.1:4161")
	w.AddLookupdNodes(lookupList)
	defer w.Stop()

	err = w.MultiPublishWithJsonExt(topicName, msgs, extList)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = w.MultiPublishWithJsonExt(topicName, badMsgs, badExtList)
	if err != nil {
		t.Fatalf(err.Error())
	}
	msgsRead := readExtMessages(topicName, t, msgCount, false)
	for i, msg := range msgsRead {
		if i%2 == 0 {
			assert.Contains(t, string(msg.ExtBytes), "{}")
			assert.Equal(t, msg.ExtVer, uint8(4))
		} else {
			assert.Equal(t, msg.ExtVer, uint8(4))
			assert.Equal(t, msg.GetTraceID(), uint64(12345))
			jsonExt, err := msg.GetJsonExt()
			if err != nil {
				t.Fatalf(err.Error())
			}
			assert.Equal(t, jsonExt.TraceID, uint64(12345))
			assert.Equal(t, jsonExt.DispatchTag, "tag123")
			assert.Equal(t, jsonExt.Custom["key1"], "val1")
			assert.Equal(t, jsonExt.Custom["key2"], "val2")
		}
	}
}

func TestTopicProducerMgrWithTagDynamicTopic(t *testing.T) {
	testTopicProducerMgrWithTag(t, true)
}

func TestTopicProducerMgrWithTagStaticTopic(t *testing.T) {
	testTopicProducerMgrWithTag(t, false)
}

func testTopicProducerMgrWithTag(t *testing.T, dynamic bool) {
	topicName := "topic_producer_mgr_publish_ext" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10
	topicList := make([]string, 0)
	for i := 0; i < 3; i++ {
		topicList = append(topicList, "t"+strconv.Itoa(i)+topicName)
		EnsureTopicWithExt(t, 4150, "t"+strconv.Itoa(i)+topicName, 0, true)
		ensureInitChannelExt(t, "t"+strconv.Itoa(i)+topicName, false)
	}

	// wait nsqd report to lookupd
	time.Sleep(time.Second * 3)

	config := NewConfig()
	initTopics := make([]string, 0)
	if !dynamic {
		initTopics = topicList
	}
	config.PubStrategy = PubRR
	config.DialTimeout = time.Second
	config.ReadTimeout = time.Second * 6
	config.HeartbeatInterval = time.Second * 3
	w, err := NewTopicProducerMgr(initTopics, config)
	if err != nil {
		t.Fatal(err)
	}
	w.SetLogger(newTestLogger(t), LogLevelInfo)
	lookupList := make([]string, 0)
	lookupList = append(lookupList, "127.0.0.1:4161")
	w.AddLookupdNodes(lookupList)
	defer w.Stop()

	var testData [][]byte
	for i := 0; i < msgCount; i++ {
		testData = append(testData, []byte("publish_tag_test_case"))
	}

	var wg sync.WaitGroup
	// test async pub
	for _, tn := range topicList {
		wg.Add(1)
		go func(tname string) {
			defer wg.Done()
			responseChan := make(chan *ProducerTransaction, msgCount)
			var jsonExt MsgExt
			jsonExt.DispatchTag = "thisIsTag1"
			for i := 0; i < msgCount; i++ {
				err := w.PublishAsyncWithJsonExt(tname, []byte("publish_test_case"), &jsonExt, responseChan, tname)
				if err != nil {
					t.Fatalf(err.Error())
				}
			}

			for i := 0; i < msgCount; i++ {
				trans := <-responseChan
				if trans.Error != nil {
					t.Fatalf(trans.Error.Error())
				}
				if trans.Args[0].(string) != tname {
					t.Fatalf(`proxied arg "%s" != "%s"`, trans.Args[0].(string), tname)
				}
			}

			go func() {
				time.Sleep(time.Second)
				var jsonExt MsgExt
				jsonExt.DispatchTag = "thisIsTag1"
				_, _, _, err := w.PublishWithJsonExt(tname, []byte("bad_test_case"), &jsonExt)
				if err != nil {
					t.Fatalf("error %s", err)
				}
			}()
			readExtMessages(tname, t, msgCount, false)
		}(tn)
	}
	wg.Wait()
}

func testTopicProducerMgr(t *testing.T, dynamic bool) {
	topicName := "topic_producer_mgr_publish" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10
	topicList := make([]string, 0)
	for i := 0; i < 3; i++ {
		topicList = append(topicList, "t"+strconv.Itoa(i)+topicName)
		EnsureTopic(t, 4150, "t"+strconv.Itoa(i)+topicName, 0)
		ensureInitChannel(t, "t"+strconv.Itoa(i)+topicName, false)
	}

	// wait nsqd report to lookupd
	time.Sleep(time.Second * 3)

	config := NewConfig()
	initTopics := make([]string, 0)
	if !dynamic {
		initTopics = topicList
	}
	config.PubStrategy = PubRR
	config.DialTimeout = time.Second
	config.ReadTimeout = time.Second * 6
	config.HeartbeatInterval = time.Second * 3
	w, err := NewTopicProducerMgr(initTopics, config)
	if err != nil {
		t.Fatal(err)
	}
	w.SetLogger(newTestLogger(t), LogLevelInfo)
	lookupList := make([]string, 0)
	lookupList = append(lookupList, "127.0.0.1:4161")
	w.AddLookupdNodes(lookupList)
	defer w.Stop()

	var testData [][]byte
	for i := 0; i < msgCount; i++ {
		testData = append(testData, []byte("multipublish_test_case"))
	}

	var wg sync.WaitGroup
	// test multipub
	for _, tn := range topicList {
		wg.Add(1)
		go func(tname string) {
			defer wg.Done()
			err := w.MultiPublish(tname, testData)
			if err != nil {
				t.Fatal("error pub :", err)
			}

			go func() {
				time.Sleep(time.Second)
				err = w.Publish(tname, []byte("bad_test_case"))
				if err != nil {
					t.Errorf("error pub %s", err)
				}
			}()

			readMessages(tname, t, msgCount, true)
		}(tn)
	}
	wg.Wait()

	// test multi async pub
	for _, tn := range topicList {
		wg.Add(1)
		go func(tname string) {
			defer wg.Done()
			responseChan := make(chan *ProducerTransaction, 1)
			err := w.MultiPublishAsync(tname, testData, responseChan, tname, 1)
			if err != nil {
				t.Fatal(err.Error())
			}

			trans := <-responseChan
			if trans.Error != nil {
				t.Error(trans.Error.Error())
			}
			if trans.Args[0].(string) != tname {
				t.Errorf(`proxied arg "%s" != "%s"`, trans.Args[0].(string), tname)
			}
			if trans.Args[1].(int) != 1 {
				t.Errorf(`proxied arg %d != 1`, trans.Args[1].(int))
			}
			go func() {
				time.Sleep(time.Second)
				err = w.Publish(tname, []byte("bad_test_case"))
				if err != nil {
					t.Fatalf("error %s", err)
				}
			}()
			readMessages(tname, t, msgCount, true)
		}(tn)
	}
	wg.Wait()

	// test async pub
	for _, tn := range topicList {
		wg.Add(1)
		go func(tname string) {
			defer wg.Done()
			responseChan := make(chan *ProducerTransaction, msgCount)
			for i := 0; i < msgCount; i++ {
				err := w.PublishAsync(tname, []byte("publish_test_case"), responseChan, tname)
				if err != nil {
					t.Fatalf(err.Error())
				}
			}

			for i := 0; i < msgCount; i++ {
				trans := <-responseChan
				if trans.Error != nil {
					t.Fatalf(trans.Error.Error())
				}
				if trans.Args[0].(string) != tname {
					t.Fatalf(`proxied arg "%s" != "%s"`, trans.Args[0].(string), tname)
				}
			}

			go func() {
				time.Sleep(time.Second)
				err := w.Publish(tname, []byte("bad_test_case"))
				if err != nil {
					t.Fatalf("error %s", err)
				}
			}()
			readMessages(tname, t, msgCount, true)
		}(tn)
	}
	wg.Wait()
}

func TestRemoveUnusedProducerAsync(t *testing.T) {
	topicName := "topic_remove_unused_producer_async" + strconv.Itoa(int(time.Now().Unix()))

	w := TopicProducerMgr{}
	w.producers = make(map[string]*producerPool)
	w.topics = make(map[string]*TopicPartProducerInfo, len(w.topics))
	w.removingProducers = make(map[string]*RemoveProducerInfo)
	addr := "127.0.0.3"
	anotherAddr := "127.0.0.4"
	newProd, _ := newProducerPool(addr, &w.config)
	anotherNewProd, _ := newProducerPool(anotherAddr, &w.config)
	newProd.SetLogger(w.getLogger())
	w.producers[addr] = newProd
	anotherNewProd.SetLogger(w.getLogger())
	w.producers[anotherAddr] = anotherNewProd
	partProducerInfo := NewTopicPartProducerInfo(metaInfo{}, false)

	allTopicParts := make([]AddrPartInfo, 0)
	addrPartInfo := AddrPartInfo{"127.0.0.3", 0}
	addrPartInfo1 := AddrPartInfo{"127.0.0.4", 1}
	allTopicParts = append(allTopicParts, addrPartInfo)
	partProducerInfo.allPartitions = append(partProducerInfo.allPartitions, addrPartInfo)
	partProducerInfo.allPartitions = append(partProducerInfo.allPartitions, addrPartInfo1)
	w.topics[topicName] = partProducerInfo
	w.removeUnusedProducerAsync(allTopicParts)
	if len(w.removingProducers) != 1 {
		t.Fatalf("TestRemoveUnusedProducerAsync removing producers num after remove expected:%d actual:%d",
			1, len(w.removingProducers))
	}
	if w.removingProducers["127.0.0.4"] == nil {
		t.Fatalf("TestRemoveUnusedProducerAsync remove wrong producers expected:%s",
			"127.0.0.4")
	}
	if len(w.producers) != 1 {
		t.Fatalf("TestRemoveUnusedProducerAsync existing producers num after remove expected:%d actual:%d",
			1, len(w.producers))
	}
	if w.producers["127.0.0.3"] == nil {
		t.Fatalf("TestRemoveUnusedProducerAsync remove wrong producers expected existing producer is :%s, but not exist",
			"127.0.0.3")
	}
	w.removingProducers["127.0.0.4"].ts = time.Now().Add(time.Minute * (-10))
	w.removeUnusedProducerAsync(allTopicParts)

	for _, p := range w.topics[topicName].allPartitions {
		if p.addr == "127.0.0.4" {
			t.Fatalf("TestRemoveUnusedProducerAsync topic partitions should be removed addr: %s but still exist",
				"127.0.0.4")
		}
	}
}

func TestQueryLookupd(t *testing.T) {
	stubFlag := false
	stubFlagMutex := sync.Mutex{}
	topicName := "topic_query_lookupd1" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10
	topicList := make([]string, 0)
	for i := 0; i < 3; i++ {
		topicList = append(topicList, "t"+strconv.Itoa(i)+topicName)
		EnsureTopic(t, 4150, "t"+strconv.Itoa(i)+topicName, 0)
		ensureInitChannel(t, "t"+strconv.Itoa(i)+topicName, false)
	}

	// wait nsqd report to lookupd
	time.Sleep(time.Second * 3)

	config := NewConfig()
	initTopics := make([]string, 0)
	config.PubStrategy = PubRR
	config.DialTimeout = time.Second
	config.ReadTimeout = time.Second * 6
	config.HeartbeatInterval = time.Second * 3
	w, err := NewTopicProducerMgr(initTopics, config)
	if err != nil {
		t.Fatal(err)
	}
	w.SetLogger(newTestLogger(t), LogLevelInfo)
	lookupList := make([]string, 0)
	lookupList = append(lookupList, "127.0.0.1:4165")
	w.AddLookupdNodes(lookupList)
	var meta metaInfo
	meta.PartitionNum = 1
	meta.Replica = 1
	stopC := make(chan struct{})
	var ensureFakedLookup = func(t *testing.T, addr string, meta metaInfo, topicList []string, stopC chan struct{}) {
		lookupdWrapper := NewNsqlookupdWrapper(t, "127.0.0.1:4161", metaInfo{
			PartitionNum:  1,
			Replica:       1,
			ExtendSupport: true,
		})
		go func() {
			srvMux := http.NewServeMux()
			srvMux.HandleFunc("/lookup", func(w http.ResponseWriter, r *http.Request) {
				stubFlagMutex.Lock()
				stubFlagTemp := stubFlag
				stubFlagMutex.Unlock()
				if stubFlagTemp {
					reqParams, err := url.ParseQuery(r.URL.RawQuery)
					if err != nil {
						t.Fatalf("error parse query %v", r.URL.RawQuery)
					}
					topic := reqParams.Get("topic")
					access := reqParams.Get("access")
					metainfo := reqParams.Get("metainfo")

					val := r.Header.Get("Accept")
					lookupdUrl := fmt.Sprintf("http://%s/lookup?topic=%s&access=%s&metainfo=%s", "127.0.0.1:4161", topic, access, metainfo)
					req, err := http.NewRequest("GET", lookupdUrl, nil)
					req.Header.Add("Accept", val)
					fmt.Printf("%v\n", val)
					client := &http.Client{}
					resp, err := client.Do(req)
					if err != nil {
						t.Fatalf("error from upstream lookup %v", err)
					}
					var nodes lookupResp
					json.NewDecoder(resp.Body).Decode(&nodes)
					defer resp.Body.Close()
					newPartitions := make(map[string]*peerInfo)
					for k, v := range nodes.Partitions {
						if strings.Contains(topic, "t0") {
							v.BroadcastAddress = "127.0.0.2"
						} else if strings.Contains(topic, "t1") {
							continue
						} else if strings.Contains(topic, "t2") {
							continue
						}
						newPartitions[k] = v
					}
					nodes.Partitions = newPartitions
					nodes.Producers = []*peerInfo{}
					jsonBytes, err := json.Marshal(&nodes)
					if err != nil {
						t.Fatalf("error parse lookup resp")
					}
					w.Header().Add("X-Nsq-Content-Type", "nsq; version=1.0")
					t.Logf("resp: %s", jsonBytes)
					w.Write(jsonBytes)
				} else {
					lookupdWrapper.lookupdWrap(w, r)
				}
			})
			srvMux.HandleFunc("/listlookup", lookupdWrapper.fakeListLookupdWrapSupplier("{\"status_code\":200,\"status_txt\":\"OK\",\"data\":{\"lookupdleader\":{\"ID\":\"127.0.0.1:4260:4160:nsqlookup\",\"NodeIP\":\"127.0.0.1\",\"TcpPort\":\"4160\",\"HttpPort\":\"4165\",\"RpcPort\":\"4260\",\"Epoch\":0},\"lookupdnodes\":[{\"ID\":\"127.0.0.1:4260:4160:nsqlookup\",\"NodeI\":\"127.0.0.1\",\"TcpPort\":\"4160\",\"HttpPort\":\"4165\",\"RpcPort\":\"4260\",\"Epoch\":0}]}}"))
			l, err := net.Listen("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			go func() {
				select {
				case <-stopC:
					l.Close()
				}
			}()
			http.Serve(l, srvMux)
		}()
		<-time.After(time.Second)
	}
	ensureFakedLookup(t, "127.0.0.1:4165", meta, topicList, stopC)
	defer func() {
		close(stopC)
		time.Sleep(time.Second)
	}()

	defer w.Stop()
	var testData [][]byte
	for i := 0; i < msgCount; i++ {
		testData = append(testData, []byte("lookupd_test_case"+strconv.Itoa(i)))
		//testData = append(testData, []byte("multipublish_test_case"))
	}
	sendMsgAndCheckResult(w, topicList, testData, msgCount, t)
	time.Sleep(time.Second * 2)
	stubFlagMutex.Lock()
	stubFlag = false
	stubFlagMutex.Unlock()
	w.queryLookupd("")
	stubFlagMutex.Lock()
	stubFlag = true
	stubFlagMutex.Unlock()
	w.queryLookupd("")
	if len(w.removingProducers) != 0 {
		for _, v := range w.removingProducers {
			v.ts = time.Now().Add(time.Minute * (-10))
		}

	} else {
		t.Fatalf("producer should be removed but actually not")
	}
	/*
	 * ensure that producer has been removed
	 */
	w.queryLookupd("")
	stubFlag = false
	sendMsgAndCheckResult(w, topicList, testData, msgCount, t)
}

func sendMsgAndCheckResult(w *TopicProducerMgr, topicList []string, testData [][]byte, msgCount int, t *testing.T) {
	var wg sync.WaitGroup
	for _, tn := range topicList {
		wg.Add(1)
		go func(tname string) {
			defer wg.Done()
			err := w.MultiPublish(tname, testData)
			if err != nil {
				t.Fatal("error pub :", err)
			}

			go func() {
				time.Sleep(time.Second)
				err = w.Publish(tname, []byte("bad_test_case"))
				if err != nil {
					t.Errorf("error pub %s", err)
				}
			}()
			readMessages(tname, t, msgCount, true)
		}(tn)
	}
	wg.Wait()
}
func readMessages(topicName string, t *testing.T, msgCount int, useLookup bool) {
	readMessages2(topicName, t, msgCount, useLookup, false)
}
func readMessages2(topicName string, t *testing.T, msgCount int, useLookup bool, cntGreater bool) {
	config := NewConfig()
	config.DefaultRequeueDelay = 0
	config.MaxBackoffDuration = 50 * time.Millisecond
	q, _ := NewConsumer(topicName, "ch", config)
	//q.SetLogger(log.New(os.Stderr, "", log.LstdFlags), LogLevelInfo)
	q.SetLogger(nullLogger, LogLevelInfo)

	h := &ConsumerHandler{
		t:           t,
		q:           q,
		checkResult: make(map[string]bool),
	}
	q.AddHandler(h)

	if useLookup {
		err := q.ConnectToNSQLookupd("127.0.0.1:4161")
		if err != nil {
			t.Fatalf(err.Error())
		}
	} else {
		err := q.ConnectToNSQD("127.0.0.1:4150", 0)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}
	select {
	case <-q.StopChan:
	case <-time.After(time.Second * 10):
		t.Errorf("timeout for consume")
		q.Stop()
	}

	if h.messagesGood != msgCount {
		if cntGreater {
			t.Logf("end of test. have handled a diff number of messages %d != %d", h.messagesGood, msgCount)
			if h.messagesGood < msgCount {
				t.Fatalf("end of test. should have handled a diff number of messages %d != %d", h.messagesGood, msgCount)
			}
		} else {
			t.Fatalf("end of test. should have handled a diff number of messages %d != %d", h.messagesGood, msgCount)
		}
	}

	if h.messagesFailed != 1 {
		t.Logf("end of test. failed messages %d", h.messagesFailed)
		if cntGreater {
			if h.messagesFailed < 1 {
				t.Fatal("failed message not done")
			}
		} else {
			t.Fatal("failed message not done")
		}
	}
}

func readExtMessages(topicName string, t *testing.T, msgCount int, useLookup bool) []*Message {
	config := NewConfig()
	config.DefaultRequeueDelay = 0
	config.MaxBackoffDuration = 50 * time.Millisecond
	q, _ := NewConsumer(topicName, "ch", config)
	q.SetConsumeExt(true)
	//q.SetLogger(log.New(os.Stderr, "", log.LstdFlags), LogLevelInfo)
	q.SetLogger(nullLogger, LogLevelInfo)

	h := &ConsumerHandler{
		t: t,
		q: q,
	}
	q.AddHandler(h)

	if useLookup {
		err := q.ConnectToNSQLookupd("127.0.0.1:4161")
		if err != nil {
			t.Fatalf(err.Error())
		}
	} else {
		err := q.ConnectToNSQD("127.0.0.1:4150", 0)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}
	select {
	case <-q.StopChan:
	case <-time.After(time.Second * 10):
		t.Errorf("timeout for consume")
		q.Stop()
	}

	if h.messagesGood != msgCount {
		t.Fatalf("end of test. should have handled a diff number of messages %d != %d", h.messagesGood, msgCount)
	}

	if h.messagesFailed != 1 {
		t.Fatal("failed message not done")
	}
	return h.Msgs
}

type mockProducerConn struct {
	delegate ConnDelegate
	closeCh  chan struct{}
	pubCh    chan struct{}
}

func newMockProducerConn(delegate ConnDelegate) producerConn {
	m := &mockProducerConn{
		delegate: delegate,
		closeCh:  make(chan struct{}),
		pubCh:    make(chan struct{}, 4),
	}
	go m.router()
	return m
}

func (m *mockProducerConn) String() string {
	return "127.0.0.1:0"
}

func (m *mockProducerConn) SetLogger(logger logger, level LogLevel, prefix string) {}

func (m *mockProducerConn) Connect() (*IdentifyResponse, error) {
	return &IdentifyResponse{}, nil
}

func (m *mockProducerConn) CloseRead() error {
	close(m.closeCh)
	return nil
}

func (m *mockProducerConn) CloseAll() {
	close(m.closeCh)
}

func (m *mockProducerConn) WriteCommand(cmd *Command) error {
	if bytes.Equal(cmd.Name, []byte("PUB")) {
		m.pubCh <- struct{}{}
	}
	return nil
}

func (m *mockProducerConn) router() {
	for {
		select {
		case <-m.closeCh:
			goto exit
		case <-m.pubCh:
			m.delegate.OnResponse(nil, framedResponse(FrameTypeResponse, []byte("OK")))
		}
	}
exit:
}

func BenchmarkProducer(b *testing.B) {
	b.StopTimer()
	body := make([]byte, 512)

	config := NewConfig()
	p, _ := NewProducer("127.0.0.1:0", config)

	p.conn = newMockProducerConn(&producerConnDelegate{p})
	atomic.StoreInt32(&p.state, StateConnected)
	p.closeChan = make(chan int)
	go p.router()

	startCh := make(chan struct{})
	var wg sync.WaitGroup
	parallel := runtime.GOMAXPROCS(0)

	for j := 0; j < parallel; j++ {
		wg.Add(1)
		go func() {
			<-startCh
			for i := 0; i < b.N/parallel; i++ {
				p.Publish("test", body)
			}
			wg.Done()
		}()
	}

	b.StartTimer()
	close(startCh)
	wg.Wait()
}
