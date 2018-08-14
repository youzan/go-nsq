package nsq

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type MyTestHandler struct {
	t                *testing.T
	q                *Consumer
	messagesSent     int
	messagesReceived int
	messagesFailed   int
	tag              string
	expectFailed     int
}

type NsqdlookupdWrapper struct {
	lookupdAddr     string
	metaInfoWrapper metaInfo
	t               *testing.T
	fakeResponse    map[string]lookupResp
}

func NewNsqlookupdWrapper(t *testing.T, addr string, metaInfo metaInfo) *NsqdlookupdWrapper {
	proxy := &NsqdlookupdWrapper{
		lookupdAddr:     addr,
		metaInfoWrapper: metaInfo,
		t:               t,
		fakeResponse:    make(map[string]lookupResp),
	}
	return proxy
}

func (self *NsqdlookupdWrapper) fakeListLookupdWrap(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("{}"))
}

func (self *NsqdlookupdWrapper) fakeLookupdWrap(w http.ResponseWriter, r *http.Request) {
	reqParams, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		self.t.Fatalf("error parse query %v", r.URL.RawQuery)
	}
	topic := reqParams.Get("topic")

	nodes, _ := self.fakeResponse[topic]
	jsonBytes, err := json.Marshal(&nodes)
	if err != nil {
		self.t.Fatalf("error parse lookup resp")
	}
	w.Header().Add("X-Nsq-Content-Type", "nsq; version=1.0")
	self.t.Logf("resp: %s", jsonBytes)
	w.Write(jsonBytes)
}

func (self *NsqdlookupdWrapper) lookupdWrap(w http.ResponseWriter, r *http.Request) {
	reqParams, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		self.t.Fatalf("error parse query %v", r.URL.RawQuery)
	}
	access := reqParams.Get("access")
	topic := reqParams.Get("topic")
	metainfo := reqParams.Get("metainfo")

	val := r.Header.Get("Accept")
	lookupdUrl := fmt.Sprintf("http://%s/lookup?topic=%s&access=%s&metainfo=%s", self.lookupdAddr, topic, access, metainfo)
	req, err := http.NewRequest("GET", lookupdUrl, nil)
	req.Header.Add("Accept", val)
	fmt.Printf("%v\n", val)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		self.t.Fatalf("error from upstream lookup %v", err)
	}
	var nodes lookupResp
	json.NewDecoder(resp.Body).Decode(&nodes)
	defer resp.Body.Close()
	nodes.Meta = self.metaInfoWrapper
	jsonBytes, err := json.Marshal(&nodes)
	if err != nil {
		self.t.Fatalf("error parse lookup resp")
	}

	w.Header().Add("X-Nsq-Content-Type", "nsq; version=1.0")
	w.Write(jsonBytes)
}

func ensureFakedLookup(t *testing.T, addr string, meta metaInfo, topicList []string, stopC chan struct{}) (*NsqdlookupdWrapper, []*peerInfo, map[string]*peerInfo) {
	lookupdWrapper := NewNsqlookupdWrapper(t, "127.0.0.1:4161", metaInfo{
		PartitionNum:  1,
		Replica:       1,
		ExtendSupport: true,
	})
	allPeers := make([]*peerInfo, 0)
	for i := 0; i < meta.PartitionNum*meta.Replica; i++ {
		var peer peerInfo
		peer.BroadcastAddress = "127.0.0.1"
		peer.Hostname = "test" + strconv.Itoa(i)
		peer.HTTPPort = 4151 + i*100
		peer.RemoteAddress = "127.0.0.1"
		peer.TCPPort = 4150 + i*100
		peer.Version = "1.0"
		allPeers = append(allPeers, &peer)
	}
	usedPeers := make(map[string]*peerInfo)
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
		lookupdWrapper.fakeResponse[t] = rsp
	}
	go func() {
		srvMux := http.NewServeMux()
		srvMux.HandleFunc("/lookup", lookupdWrapper.fakeLookupdWrap)
		srvMux.HandleFunc("/listlookup", lookupdWrapper.fakeListLookupdWrap)
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
	return lookupdWrapper, allPeers, usedPeers
}

var nullLogger = log.New(ioutil.Discard, "", log.LstdFlags)

func (h *MyTestHandler) LogFailedMessage(message *Message) {
	h.messagesFailed++
	h.t.Logf("log fail message: %v, %v", h.messagesReceived, GetNewMessageID(message.ID[:]))
	if h.messagesFailed >= h.expectFailed {
		h.q.Stop()
	}
}

func (h *MyTestHandler) HandleMessage(message *Message) error {
	if string(message.Body) == "TOBEFAILED" {
		h.messagesReceived++
		h.t.Logf("received fail message: %v, %v", h.messagesReceived, GetNewMessageID(message.ID[:]))
		return errors.New("fail this message")
	}

	data := struct {
		Msg string
	}{}

	err := json.Unmarshal(message.Body, &data)
	if err != nil {
		return err
	}

	msg := data.Msg
	if msg != "single" && msg != "double" {
		h.t.Error("message 'action' was not correct: ", msg, data)
	}
	if h.tag != "" {
		if message.ExtVer != uint8(4) || !strings.Contains(string(message.ExtBytes), h.tag) {
			h.t.Error("message received has different tag or ext version: ", h.tag, message.ExtVer, string(message.ExtBytes))
		}
	}
	h.messagesReceived++
	h.t.Logf("received message: %v, %v", h.messagesReceived, GetNewMessageID(message.ID[:]))
	return nil
}

func EnsureTopicWithExt(t *testing.T, port int, topic string, part int, ext bool) {
	endpoint := fmt.Sprintf("127.0.0.1:%d", port)
	conn, err := net.DialTimeout("tcp", endpoint, 3*time.Second)
	if err != nil {
		t.Fatalf(err.Error())
		return
	}
	conn.Write(MagicV2)
	if ext {
		CreateTopicWithExt(topic, part).WriteTo(conn)
	} else {
		CreateTopic(topic, part).WriteTo(conn)
	}
	resp, err := ReadResponse(conn)
	if err != nil {
		t.Fatal(err.Error())
		return
	}
	frameType, data, err := UnpackResponse(resp)
	if err != nil {
		t.Fatal(err.Error())
		return
	}
	if frameType == FrameTypeError {
		t.Fatal(string(data))
	}
	time.Sleep(time.Second)
}

func EnsureTopic(t *testing.T, port int, topic string, part int) {
	endpoint := fmt.Sprintf("127.0.0.1:%d", port)
	conn, err := net.DialTimeout("tcp", endpoint, 3*time.Second)
	if err != nil {
		t.Fatalf(err.Error())
		return
	}
	conn.Write(MagicV2)
	CreateTopic(topic, part).WriteTo(conn)
	resp, err := ReadResponse(conn)
	if err != nil {
		t.Fatal(err.Error())
		return
	}
	frameType, data, err := UnpackResponse(resp)
	if err != nil {
		t.Fatal(err.Error())
		return
	}
	if frameType == FrameTypeError {
		t.Fatal(string(data))
	}
	time.Sleep(time.Second)
}

func SendMessage(t *testing.T, port int, topic string, method string, body []byte) {
	httpclient := &http.Client{}
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/%s?topic=%s", port, method, topic)
	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(body))
	resp, err := httpclient.Do(req)
	if err != nil {
		t.Fatalf(err.Error())
		return
	}
	resp.Body.Close()
}

func SendTagedMessage(t *testing.T, port int, topic string, method string, body []byte, tag string) {
	httpclient := &http.Client{}
	jsonStr := fmt.Sprintf("{\"##client_dispatch_tag\":\"%s\"}", tag)
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/%s?topic=%s&ext=%s", port, method, topic, url.QueryEscape(jsonStr))
	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(body))
	resp, err := httpclient.Do(req)
	if err != nil {
		t.Fatalf(err.Error())
		return
	}
	resp.Body.Close()
}

func TestConsumer(t *testing.T) {
	consumerTest(t, nil)
}

func TestConsumerTLS(t *testing.T) {
	consumerTest(t, func(c *Config) {
		c.TlsV1 = true
		c.TlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	})
}

func TestConsumerDeflate(t *testing.T) {
	consumerTest(t, func(c *Config) {
		c.Deflate = true
	})
}

func TestConsumerSnappy(t *testing.T) {
	consumerTest(t, func(c *Config) {
		c.Snappy = true
	})
}

func TestConsumerTLSDeflate(t *testing.T) {
	consumerTest(t, func(c *Config) {
		c.TlsV1 = true
		c.TlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
		c.Deflate = true
	})
}

func TestConsumerTLSSnappy(t *testing.T) {
	consumerTest(t, func(c *Config) {
		c.TlsV1 = true
		c.TlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
		c.Snappy = true
	})
}

func TestConsumerTLSClientCert(t *testing.T) {
	envDl := os.Getenv("NSQ_DOWNLOAD")
	if strings.HasPrefix(envDl, "nsq-0.2.24") || strings.HasPrefix(envDl, "nsq-0.2.27") {
		t.Log("skipping due to older nsqd")
		return
	}
	cert, _ := tls.LoadX509KeyPair("./test/client.pem", "./test/client.key")
	consumerTest(t, func(c *Config) {
		c.TlsV1 = true
		c.TlsConfig = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true,
		}
	})
}

func TestConsumerTLSClientCertViaSet(t *testing.T) {
	envDl := os.Getenv("NSQ_DOWNLOAD")
	if strings.HasPrefix(envDl, "nsq-0.2.24") || strings.HasPrefix(envDl, "nsq-0.2.27") {
		t.Log("skipping due to older nsqd")
		return
	}
	consumerTest(t, func(c *Config) {
		c.Set("tls_v1", true)
		c.Set("tls_cert", "./test/client.pem")
		c.Set("tls_key", "./test/client.key")
		c.Set("tls_insecure_skip_verify", true)
	})
}

func TestConsumerDiscoveryLookupd(t *testing.T) {
	// TODO:
}

func TestConsumerSubToNotLeader(t *testing.T) {
	// TODO:
}

func TestConsumerSubToExtTopic(t *testing.T) {
	consumerTagTest(t, func(c *Config) {
		c.Set("desired_tag", "tagTest123")
	})
}

func TestConsumerSubToExtTopicLookupd(t *testing.T) {
	consumerTagTestLookupd(t, func(c *Config) {
		c.Set("desired_tag", "tagTest123")
	})
}

func consumerTagTestLookupd(t *testing.T, cb func(c *Config)) {
	lookupdAddrWrapper := "127.0.0.1:4162"
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		lookupdWrapper := NewNsqlookupdWrapper(t, "127.0.0.1:4161", metaInfo{
			PartitionNum:  1,
			Replica:       1,
			ExtendSupport: true,
		})

		srvMux := http.NewServeMux()
		srvMux.HandleFunc("/lookup", lookupdWrapper.lookupdWrap)
		//http.ListenAndServe(lookupdAddrWrapper, nil)
		l, err := net.Listen("tcp", lookupdAddrWrapper)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			select {
			case <-stopCh:
				l.Close()
			}
		}()
		http.Serve(l, srvMux)
	}()
	<-time.After(2 * time.Second)
	fmt.Printf("nsqlookupd starts.")

	config := NewConfig()
	laddr := "127.0.0.1"
	// so that the test can simulate binding consumer to specified address
	config.LocalAddr, _ = net.ResolveTCPAddr("tcp", laddr+":0")
	// so that the test can simulate reaching max requeues and a call to LogFailedMessage
	config.DefaultRequeueDelay = 0
	config.MaxAttempts = 5
	// so that the test wont timeout from backing off
	config.MaxBackoffDuration = time.Millisecond * 50
	if cb != nil {
		cb(config)
	}
	config.LookupdPollInterval = time.Second
	tag := config.DesiredTag
	//rest desired tag to default
	config.DesiredTag = ""
	topicName := "rdr_tag_test_lookupd"
	if config.Deflate {
		topicName = topicName + "_deflate"
	} else if config.Snappy {
		topicName = topicName + "_snappy"
	}
	if config.TlsV1 {
		topicName = topicName + "_tls"
	}
	topicName = topicName + strconv.Itoa(int(time.Now().Unix()))
	EnsureTopicWithExt(t, 4150, topicName, 0, true)

	q, _ := NewConsumer(topicName, "ch", config)
	q.SetLogger(newTestLogger(t), LogLevelDebug)

	h := &MyTestHandler{
		t:            t,
		q:            q,
		expectFailed: 2,
	}
	q.AddHandler(h)
	q.ConnectToNSQLookupd(lookupdAddrWrapper)

	SendTagedMessage(t, 4151, topicName, "pub", []byte(`{"msg":"single"}`), tag)
	SendMessage(t, 4151, topicName, "pub", []byte(`{"msg":"single"}`))
	SendTagedMessage(t, 4151, topicName, "pub", []byte(`{"msg":"single"}`), tag)
	SendMessage(t, 4151, topicName, "pub", []byte(`{"msg":"single"}`))
	SendTagedMessage(t, 4151, topicName, "pub", []byte(`{"msg":"single"}`), tag)
	SendMessage(t, 4151, topicName, "pub", []byte(`{"msg":"single"}`))
	SendTagedMessage(t, 4151, topicName, "pub", []byte("TOBEFAILED"), tag)
	SendMessage(t, 4151, topicName, "pub", []byte("TOBEFAILED"))
	h.messagesSent = 8

	select {
	case <-time.After(time.Second * 20):
		t.Errorf("tag consumer should stop after timeout")
		q.Stop()
	case <-q.StopChan:
	}

	if h.messagesReceived != h.messagesSent+h.messagesFailed*int(config.MaxAttempts-1) {
		t.Fatalf("end of test. should have handled a diff number of messages (got %d, sent %d)", h.messagesReceived, h.messagesSent)
	}
	if h.messagesFailed != h.expectFailed {
		t.Fatal("failed message not done")
	}
	close(stopCh)
	wg.Wait()
}

func consumerTagTest(t *testing.T, cb func(c *Config)) {
	config := NewConfig()
	laddr := "127.0.0.1"
	// so that the test can simulate binding consumer to specified address
	config.LocalAddr, _ = net.ResolveTCPAddr("tcp", laddr+":0")
	// so that the test can simulate reaching max requeues and a call to LogFailedMessage
	config.DefaultRequeueDelay = 0
	// so that the test wont timeout from backing off
	config.MaxBackoffDuration = time.Millisecond * 50
	config.MaxAttempts = 5
	if cb != nil {
		cb(config)
	}
	tag := config.DesiredTag
	topicName := "rdr_tag_test"
	if config.Deflate {
		topicName = topicName + "_deflate"
	} else if config.Snappy {
		topicName = topicName + "_snappy"
	}
	if config.TlsV1 {
		topicName = topicName + "_tls"
	}
	config.LookupdPollInterval = time.Second

	topicName = topicName + strconv.Itoa(int(time.Now().Unix()))
	EnsureTopicWithExt(t, 4150, topicName, 0, true)

	q, _ := NewConsumer(topicName, "ch", config)
	q.SetLogger(newTestLogger(t), LogLevelDebug)

	h := &MyTestHandler{
		t:            t,
		q:            q,
		tag:          tag,
		expectFailed: 1,
	}
	q.AddHandler(h)

	addr := "127.0.0.1:4150"
	//test with invalid tag
	q.SetConsumeExt(true)
	err := q.ConnectToNSQD(addr, 2)
	if err == nil {
		time.Sleep(time.Second)
		// should call on error
		if len(q.connections) > 0 || len(q.nsqdTCPAddrs) > 0 {
			t.Fatal("should disconnect from the NSQ with not exist partition !!!")
		}
	}

	err = q.ConnectToNSQD(addr, 0)
	if err != nil {
		t.Fatal(err)
	}

	//remove desired tag in new consumer
	config.DesiredTag = ""
	qn, _ := NewConsumer(topicName, "ch", config)
	qn.SetConsumeExt(true)
	qn.SetLogger(newTestLogger(t), LogLevelDebug)

	hn := &MyTestHandler{
		t:            t,
		q:            qn,
		expectFailed: 1,
	}
	qn.AddHandler(hn)
	err = qn.ConnectToNSQD(addr, 0)
	if err != nil {
		t.Fatal(err)
	}

	SendTagedMessage(t, 4151, topicName, "pub_ext", []byte(`{"msg":"single"}`), tag)
	SendMessage(t, 4151, topicName, "pub", []byte(`{"msg":"single"}`))
	SendTagedMessage(t, 4151, topicName, "pub_ext", []byte(`{"msg":"single"}`), tag)
	SendMessage(t, 4151, topicName, "pub", []byte(`{"msg":"single"}`))
	SendTagedMessage(t, 4151, topicName, "pub_ext", []byte(`{"msg":"single"}`), tag)
	SendMessage(t, 4151, topicName, "pub", []byte(`{"msg":"single"}`))
	SendTagedMessage(t, 4151, topicName, "pub_ext", []byte("TOBEFAILED"), tag)
	SendMessage(t, 4151, topicName, "pub", []byte("TOBEFAILED"))
	h.messagesSent = 4
	hn.messagesSent = 4

	stats := q.Stats()
	if stats.Connections == 0 {
		t.Fatal("stats report 0 connections (should be > 0)")
	}

	err = q.ConnectToNSQD(addr, 0)
	if err == nil {
		t.Fatal("should not be able to connect to the same NSQ twice")
	}

	conn := q.conns()[0]
	if !strings.HasPrefix(conn.conn.LocalAddr().String(), laddr) {
		t.Fatal("connection should be bound to the specified address:", conn.conn.LocalAddr())
	}

	err = q.DisconnectFromNSQD("1.2.3.4:4150", "0")
	if err == nil {
		t.Fatal("should not be able to disconnect from an unknown nsqd")
	}

	err = q.ConnectToNSQD("1.2.3.4:4150", 0)
	if err == nil {
		t.Fatal("should not be able to connect to non-existent nsqd")
	}

	err = q.DisconnectFromNSQD("1.2.3.4:4150", "0")
	if err != nil {
		t.Fatal("should be able to disconnect from an nsqd - " + err.Error())
	}

	select {
	case <-time.After(time.Second * 20):
		t.Errorf("tag consumer should stop after timeout")
		q.Stop()
	case <-q.StopChan:
	}

	select {
	case <-time.After(time.Second * 20):
		t.Errorf("tag consumer should stop after timeout")
		qn.Stop()
	case <-qn.StopChan:
	}

	stats = q.Stats()
	if stats.Connections != 0 {
		t.Fatalf("stats report %d active connections (should be 0), %v", stats.Connections, q.conns())
	}

	stats = q.Stats()
	if stats.MessagesReceived != uint64(h.messagesReceived+h.messagesFailed) {
		t.Fatalf("stats report %d messages received (should be %d)",
			stats.MessagesReceived,
			h.messagesReceived+h.messagesFailed)
	}

	if h.messagesReceived != h.messagesSent+h.messagesFailed*int(config.MaxAttempts-1) {
		t.Fatalf("end of test. should have handled a diff number of messages (got %d, sent %d)", h.messagesReceived, h.messagesSent)
	}
	if h.messagesFailed != h.expectFailed {
		t.Fatal("failed message not done")
	}

	stats = qn.Stats()
	if stats.Connections != 0 {
		t.Fatalf("stats report %d active connections (should be 0)", stats.Connections)
	}

	stats = qn.Stats()
	if stats.MessagesReceived != uint64(hn.messagesReceived+hn.messagesFailed) {
		t.Fatalf("stats report %d messages received (should be %d)",
			stats.MessagesReceived,
			hn.messagesReceived+hn.messagesFailed)
	}

	if hn.messagesReceived != hn.messagesSent+hn.messagesFailed*(int(config.MaxAttempts)-1) {
		t.Fatalf("end of test. should have handled a diff number of messages (got %d, sent %d)", hn.messagesReceived, hn.messagesSent)
	}
	if hn.messagesFailed != hn.expectFailed {
		t.Fatal("failed message not done")
	}
}

func consumerTest(t *testing.T, cb func(c *Config)) {
	config := NewConfig()
	laddr := "127.0.0.1"
	// so that the test can simulate binding consumer to specified address
	config.LocalAddr, _ = net.ResolveTCPAddr("tcp", laddr+":0")
	// so that the test can simulate reaching max requeues and a call to LogFailedMessage
	config.DefaultRequeueDelay = 0
	// so that the test wont timeout from backing off
	config.MaxBackoffDuration = time.Millisecond * 50
	config.MaxAttempts = 7
	config.LookupdPollInterval = time.Second
	if cb != nil {
		cb(config)
	}
	topicName := "rdr_test"
	if config.Deflate {
		topicName = topicName + "_deflate"
	} else if config.Snappy {
		topicName = topicName + "_snappy"
	}
	if config.TlsV1 {
		topicName = topicName + "_tls"
	}
	topicName = topicName + strconv.Itoa(int(time.Now().Unix()))
	q, _ := NewConsumer(topicName, "ch", config)
	q.SetLogger(newTestLogger(t), LogLevelDebug)

	h := &MyTestHandler{
		t:            t,
		q:            q,
		expectFailed: 1,
	}
	q.AddHandler(h)

	EnsureTopic(t, 4150, topicName, 0)

	addr := "127.0.0.1:4150"
	err := q.ConnectToNSQD(addr, 2)
	if err == nil {
		time.Sleep(time.Second)
		// should call on error
		if len(q.connections) > 0 || len(q.nsqdTCPAddrs) > 0 {
			t.Fatal("should disconnect from the NSQ with not exist partition !!!")
		}
	}

	err = q.ConnectToNSQD(addr, 0)
	if err != nil {
		t.Fatal(err)
	}
	SendMessage(t, 4151, topicName, "pub", []byte(`{"msg":"single"}`))
	SendMessage(t, 4151, topicName, "mpub", []byte("{\"msg\":\"double\"}\n{\"msg\":\"double\"}"))
	SendMessage(t, 4151, topicName, "pub", []byte("TOBEFAILED"))
	h.messagesSent = 4

	stats := q.Stats()
	if stats.Connections == 0 {
		t.Fatal("stats report 0 connections (should be > 0)")
	}

	err = q.ConnectToNSQD(addr, 0)
	if err == nil {
		t.Fatal("should not be able to connect to the same NSQ twice")
	}

	conn := q.conns()[0]
	if !strings.HasPrefix(conn.conn.LocalAddr().String(), laddr) {
		t.Fatal("connection should be bound to the specified address:", conn.conn.LocalAddr())
	}

	err = q.DisconnectFromNSQD("1.2.3.4:4150", "0")
	if err == nil {
		t.Fatal("should not be able to disconnect from an unknown nsqd")
	}

	err = q.ConnectToNSQD("1.2.3.4:4150", 0)
	if err == nil {
		t.Fatal("should not be able to connect to non-existent nsqd")
	}

	err = q.DisconnectFromNSQD("1.2.3.4:4150", "0")
	if err != nil {
		t.Fatal("should be able to disconnect from an nsqd - " + err.Error())
	}

	select {
	case <-q.StopChan:
	case <-time.After(time.Second * 10):
		t.Errorf("should stop after timeout")
		q.Stop()
	}

	stats = q.Stats()
	if stats.Connections != 0 {
		t.Fatalf("stats report %d active connections (should be 0)", stats.Connections)
	}

	stats = q.Stats()
	if stats.MessagesReceived != uint64(h.messagesReceived+h.messagesFailed) {
		t.Fatalf("stats report %d messages received (should be %d)",
			stats.MessagesReceived,
			h.messagesReceived+h.messagesFailed)
	}

	if h.messagesReceived != h.messagesSent+int(config.MaxAttempts)-1 {
		t.Fatalf("end of test. should have handled a diff number of messages (got %d, sent %d)", h.messagesReceived, h.messagesSent)
	}
	if h.messagesFailed != h.expectFailed {
		t.Fatal("failed message not done")
	}
}
