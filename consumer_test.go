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
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

type MyTestHandler struct {
	t                *testing.T
	q                *Consumer
	messagesSent     int
	messagesReceived int
	messagesFailed   int
	tag		  string
}

var nullLogger = log.New(ioutil.Discard, "", log.LstdFlags)

func (h *MyTestHandler) LogFailedMessage(message *Message) {
	h.messagesFailed++
	h.q.Stop()
}

func (h *MyTestHandler) HandleMessage(message *Message) error {
	if string(message.Body) == "TOBEFAILED" {
		h.messagesReceived++
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
		if message.ExtVer != uint8(2) || string(message.ExtContext) != h.tag {
			h.t.Error("message received has different tag or ext version: ", h.tag, message.ExtVer, string(message.ExtContext))
		}
	} else {
		if message.ExtVer == uint8(2) {
			h.t.Error("message received should not has tag or tag ext version: ", message.ExtVer, string(message.ExtContext))
		}
	}
	h.messagesReceived++
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
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/%s?topic=%s&tag=%s", port, method, topic, tag)
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
	envDl := os.Getenv("NSQ_DOWNLOAD")
	if strings.HasPrefix(envDl, "nsq-0.2.24") || strings.HasPrefix(envDl, "nsq-0.2.27") {
		t.Log("skipping due to older nsqd")
		return
	}
	consumerTagTest(t, func(c *Config) {
		c.Set("desired_tag", "tagTest123")
	})
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
	topicName = topicName + strconv.Itoa(int(time.Now().Unix()))
	q, _ := NewConsumer(topicName, "ch", config)
	q.SetLogger(newTestLogger(t), LogLevelDebug)

	h := &MyTestHandler{
		t: t,
		q: q,
		tag:tag,
	}
	q.AddHandler(h)

	EnsureTopicWithExt(t, 4150, topicName, 0, true)

	addr := "127.0.0.1:4150"
	err := q.ConnectToNSQDWithExt(addr, 2, true)
	if err == nil {
		time.Sleep(time.Second)
		// should call on error
		if len(q.connections) > 0 || len(q.nsqdTCPAddrs) > 0 {
			t.Fatal("should disconnect from the NSQ with not exist partition !!!")
		}
	}

	err = q.ConnectToNSQDWithExt(addr, 0, true)
	if err != nil {
		t.Fatal(err)
	}

	//remove desired tag in new consumer
	config.DesiredTag = "";
	qn, _ := NewConsumer(topicName, "ch", config)
	qn.SetLogger(newTestLogger(t), LogLevelDebug)

	hn := &MyTestHandler{
		t: t,
		q: qn,
	}
	qn.AddHandler(hn)
	err = qn.ConnectToNSQDWithExt(addr, 0, true)
	if err != nil {
		t.Fatal(err)
	}

	SendTagedMessage(t, 4151, topicName, "pub", []byte(`{"msg":"single"}`), tag)
	SendMessage(t, 4151, topicName, "pub", []byte(`{"msg":"single"}`))
	SendTagedMessage(t, 4151, topicName, "pub", []byte(`{"msg":"single"}`), tag)
	SendMessage(t, 4151, topicName, "pub", []byte(`{"msg":"single"}`))
	SendTagedMessage(t, 4151, topicName, "pub", []byte(`{"msg":"single"}`), tag)
	SendMessage(t, 4151, topicName, "pub", []byte(`{"msg":"single"}`))
	SendTagedMessage(t, 4151, topicName, "pub", []byte("TOBEFAILED"), tag)
	SendMessage(t, 4151, topicName, "pub", []byte("TOBEFAILED"))
	h.messagesSent = 4
	hn.messagesSent = 4

	stats := q.Stats()
	if stats.Connections == 0 {
		t.Fatal("stats report 0 connections (should be > 0)")
	}

	err = q.ConnectToNSQDWithExt(addr, 0, true)
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

	err = q.ConnectToNSQDWithExt("1.2.3.4:4150", 0, true)
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
	case <-q.StopChan:
	}

	select {
	case <-time.After(time.Second * 20):
		t.Errorf("tag consumer should stop after timeout")
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

	if h.messagesReceived != 8 || h.messagesSent != 4 {
		t.Fatalf("end of test. should have handled a diff number of messages (got %d, sent %d)", h.messagesReceived, h.messagesSent)
	}
	if h.messagesFailed != 1 {
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

	if hn.messagesReceived != 8 || hn.messagesSent != 4 {
		t.Fatalf("end of test. should have handled a diff number of messages (got %d, sent %d)", hn.messagesReceived, hn.messagesSent)
	}
	if hn.messagesFailed != 1 {
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
		t: t,
		q: q,
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

	if h.messagesReceived != 8 || h.messagesSent != 4 {
		t.Fatalf("end of test. should have handled a diff number of messages (got %d, sent %d)", h.messagesReceived, h.messagesSent)
	}
	if h.messagesFailed != 1 {
		t.Fatal("failed message not done")
	}
}
