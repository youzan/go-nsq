package nsq

import (
	"bytes"
	"errors"
	"io/ioutil"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type ConsumerHandler struct {
	t              *testing.T
	q              *Consumer
	messagesGood   int
	messagesFailed int
}

func (h *ConsumerHandler) LogFailedMessage(message *Message) {
	h.messagesFailed++
	h.q.Stop()
}

func (h *ConsumerHandler) HandleMessage(message *Message) error {
	msg := string(message.Body)
	if msg == "bad_test_case" {
		return errors.New("fail this message")
	}
	if msg != "multipublish_test_case" && msg != "publish_test_case" {
		h.t.Error("message 'action' was not correct:", msg)
	}
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

	responseChan := make(chan *ProducerTransaction)
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
			responseChan := make(chan *ProducerTransaction)
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

func readMessages(topicName string, t *testing.T, msgCount int, useLookup bool) {
	config := NewConfig()
	config.DefaultRequeueDelay = 0
	config.MaxBackoffDuration = 50 * time.Millisecond
	q, _ := NewConsumer(topicName, "ch", config)
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
}

func readExtMessages(topicName string, t *testing.T, msgCount int, useLookup bool) {
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

func (m *mockProducerConn) Close() error {
	close(m.closeCh)
	return nil
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
