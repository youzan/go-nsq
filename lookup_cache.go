package nsq

import (
	"errors"
	"fmt"
	"github.com/patrickmn/go-cache"
	"github.com/ztrue/shutdown"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type LookupAccess string

const ACC_W = LookupAccess("w")
const ACC_R = LookupAccess("r")

const CLOSING = uint32(1)
const INIT = uint32(0)

const PATH_LISTLOOKUP = "/listlookup"
const PATH_LOOKUP = "/lookup"

const PARAM_TOPIC = "topic"
const PARAM_META = "metainfo"
const PARAM_ACC = "access"


var ErrLookupCacheReject = errors.New("lookup request rejected as lookup cache closed")

var N LookupCache

func init() {
	N = NewLookupCache(10 * time.Second, queryLookupEndPoint, queryListLookupEndPoint)
}

func queryListLookupEndPoint(lookupEndpoint string) (interface{}, error) {
	var data lookupListResp

	//build endpoint
	urlString := lookupEndpoint
	if !strings.Contains(urlString, "://") {
		urlString = "http://" + lookupEndpoint
	}

	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}

	if u.Path == "/" || u.Path == "" {
		u.Path = PATH_LISTLOOKUP
	}

	err = apiRequestNegotiateV1("GET", u.String(), nil, &data)
	if err != nil {
		return nil, err
	}
	return &data, nil
}

func queryLookupEndPoint(lookupEndpoint string, topic string, acc LookupAccess, metaInfo bool) (interface{}, error) {
	var data lookupResp

	//build endpoint
	urlString := lookupEndpoint
	if !strings.Contains(urlString, "://") {
		urlString = "http://" + lookupEndpoint
	}

	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}

	if u.Path == "/" || u.Path == "" {
		u.Path = PATH_LOOKUP
	}

	v, err := url.ParseQuery(u.RawQuery)
	v.Add(PARAM_TOPIC, topic)
	if metaInfo {
		v.Add(PARAM_META, "true")
	} else {
		v.Add(PARAM_META, "false")
	}
	v.Add(PARAM_ACC, string(acc))
	u.RawQuery = v.Encode()

	err = apiRequestNegotiateV1("GET", u.String(), nil, &data)
	if err != nil {
		return nil, err
	}
	return &data, nil
}

type lookupQuery struct {
	sync.WaitGroup
	Topic string
	EndPoint string
	Acc LookupAccess
	MetaInfo bool
	Resp interface{}
	Err error
}

type listLookupQuery struct {
	sync.WaitGroup
	EndPoint string
	Resp interface{}
	Err error
}

func newLookupQuery(endPoint string, topic string, acc LookupAccess, metaInfo bool) *lookupQuery {
	q := &lookupQuery{
		Topic: topic,
		Acc: acc,
		MetaInfo: metaInfo,
		EndPoint: endPoint,
	}
	q.Add(1)
	return q
}

func newListLookupQuery(endPoint string) *listLookupQuery {
	q := &listLookupQuery{
		EndPoint: endPoint,
	}
	q.Add(1)
	return q
}

type LookupCache interface {
	Lookup(endPoint string, topic string, acc LookupAccess, metaInfo bool) (interface{}, error)
	ListLookup(endPoint string) (interface{}, error)
	Stop()
}

type LookupCacheImpl struct {
	cache *cache.Cache
	//loading function for loading lookup resp
	lookupLoadingFunc func(string, string, LookupAccess, bool)(interface{}, error)
	listLookupLoadingFunc func(string)(interface{}, error)
	responderKick sync.Once
	qch chan *lookupQuery
	lqch chan *listLookupQuery
	cls chan bool
	closing uint32
	w sync.WaitGroup
}

//lookup cache key, format of topic and access
func buildLookupCacheKey(q *lookupQuery) string {
	return fmt.Sprintf("lookup:%s/%s/%s", q.EndPoint, q.Topic, q.Acc)
}

func buildListLookupCacheKey(q *listLookupQuery) string {
	return fmt.Sprintf("listlookup:%s", q.EndPoint)
}

func NewLookupCache(duration time.Duration, lookupLoadingFunc func(string, string, LookupAccess, bool)(interface{}, error), listLookupLoadingFunc func(string) (interface{}, error)) LookupCache {
	c := &LookupCacheImpl{
		lookupLoadingFunc: lookupLoadingFunc,
		listLookupLoadingFunc: listLookupLoadingFunc,
		qch: make(chan *lookupQuery, 10),
		lqch: make(chan *listLookupQuery, 10),
		cls: make(chan bool),
		closing: INIT,
	}
	c.cache = cache.New(duration, 10 * time.Second)
	return c
}

func (c *LookupCacheImpl) kickOffResponders() {
	//shutdown hook
	shutdown.Add(func(){
		c.Stop()
	})

	c.kickOffListLookupResponder()
	c.kickOffLookupResponder()
}

func (c *LookupCacheImpl) kickOffListLookupResponder() {
	go func(){
		c.w.Add(1)
		for {
			select {
			case <- c.cls:
				goto exit
			case q := <- c.lqch:
				key := buildListLookupCacheKey(q)
				if v, ok := c.cache.Get(key); ok {
					q.Resp = v
				} else {
					//perform request from loading func
					ret, err := c.listLookupLoadingFunc(q.EndPoint)
					if err != nil {
						q.Err = err
					} else if ret != nil {
						c.cache.SetDefault(key, ret)
						q.Resp = ret
					}
				}
				q.Done()
			}
		}
	exit:
		//drain lqch
		t := time.NewTimer(time.Second)
		for {
			select {
			case q := <-c.lqch:
				q.Err = ErrLookupCacheReject
				q.Done()
			case <- t.C:
				goto drainDone
			}
		}
	drainDone:
		close(c.lqch)
		c.w.Done()
	}()
}

func (c *LookupCacheImpl) kickOffLookupResponder() {
	go func(){
		c.w.Add(1)
		for {
			select {
			case <- c.cls:
				goto exit
			case q := <- c.qch:
				key := buildLookupCacheKey(q)
				if v, ok := c.cache.Get(key); ok {
					q.Resp = v
				} else {
					//perform request from loading func
					ret, err := c.lookupLoadingFunc(q.EndPoint, q.Topic, q.Acc, q.MetaInfo)
					if err != nil {
						q.Err = err
					} else if ret != nil {
						c.cache.SetDefault(key, ret)
						q.Resp = ret
					}
				}
				q.Done()
			}
		}
	exit:
		//drain qch
		t := time.NewTimer(time.Second)
		for {
			select {
			case q := <-c.qch:
				q.Err = ErrLookupCacheReject
				q.Done()
			case <- t.C:
				goto drainDone
			}
		}
	drainDone:
		close(c.qch)
		c.w.Done()
	}()
}

func Lookup(lookupAddr string, topic string, acc LookupAccess, metaInfo bool) (interface{}, error) {
	return N.Lookup(lookupAddr, topic, acc, metaInfo)
}

func ListLookup(lookupAddr string) (interface{}, error) {
	return N.ListLookup(lookupAddr)
}

func (c *LookupCacheImpl) ListLookup(endPoint string) (interface{}, error) {
	if atomic.LoadUint32(&c.closing) != INIT {
		return nil, ErrLookupCacheReject
	}
	c.responderKick.Do(c.kickOffResponders)
	llq := newListLookupQuery(endPoint)
	c.lqch <- llq
	llq.Wait()
	if llq.Err != nil {
		return nil, llq.Err
	} else {
		return llq.Resp, nil
	}
}

func (c *LookupCacheImpl) Lookup(endPoint string, topic string, acc LookupAccess, metaInfo bool) (interface{}, error) {
	if atomic.LoadUint32(&c.closing) != INIT {
		return nil, ErrLookupCacheReject
	}
	c.responderKick.Do(c.kickOffResponders)
	lq := newLookupQuery(endPoint, topic, acc, metaInfo)
	c.qch <- lq
	lq.Wait()
	if lq.Err != nil {
		return nil, lq.Err
	} else {
		return lq.Resp, nil
	}
}

func (c *LookupCacheImpl) Stop() {
	if atomic.CompareAndSwapUint32(&c.closing, INIT, CLOSING) {
		close(c.cls)
		c.w.Wait()
	}
}
