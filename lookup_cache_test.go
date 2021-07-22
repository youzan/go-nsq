package nsq

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
	"time"
)

func TestLookupCacheStop(t *testing.T) {
	cache := NewLookupCache(10 * time.Second, queryLookupEndPoint, queryListLookupEndPoint)

	//local lookup, returns not found
	lookupAddr := "127.0.0.1:4161"
	_, err := cache.ListLookup(lookupAddr)
	assert.NotNil(t, err)

	cache.Stop()

	_, err = cache.ListLookup(lookupAddr)
	assert.NotNil(t, err)
	assert.Equal(t, ErrLookupCacheReject, err)
}

type MockQueryFuncHolder struct {
	expLookupCnt int32
	lookupLoadingFunc func(lookupEndpoint string, topic string, acc LookupAccess, metaInfo bool) (interface{}, error)
	expListLookupCnt int32
	listlookupLoadingFunc func(lookupEndpoint string) (interface{}, error)
	tb testing.TB
}

var ErrLookupCntExceed = errors.New("lookup cnt exceed")
var ErrListLookupCntExceed = errors.New("listlookup cnt exceed")

func (f *MockQueryFuncHolder) lookupQFunc(lookupEndpoint string, topic string, acc LookupAccess, metaInfo bool) (interface{}, error) {
	if atomic.AddInt32(&f.expLookupCnt, -1) < 0 {
		f.tb.Error("exp lookup cnt exceed")
		return nil, ErrLookupCntExceed
	} else {
		return f.lookupLoadingFunc(lookupEndpoint, topic, acc, metaInfo)
	}
}

func (f *MockQueryFuncHolder) listlookupQFunc(lookupEndpoint string) (interface{}, error) {
	if atomic.AddInt32(&f.expListLookupCnt, -1) < 0 {
		f.tb.Error("exp listlookup cnt exceed")
		return nil, ErrListLookupCntExceed
	} else {
		return f.listlookupLoadingFunc(lookupEndpoint)
	}
}

func (f *MockQueryFuncHolder) verify() {
	if atomic.LoadInt32(&f.expLookupCnt) > 0 {
		f.tb.Error("exp lookup cnt exceed")
	}

	if atomic.LoadInt32(&f.expListLookupCnt) > 0 {
		f.tb.Error("exp listlookup cnt exceed")
	}
}

func TestLookupCacheLookup(t *testing.T) {
	topic := "lookup_cache_test"
	//create topic
	EnsureTopic(t, 4151, topic, -1)
	mfh := &MockQueryFuncHolder {
		expLookupCnt: 1,
		expListLookupCnt: 0,
		lookupLoadingFunc: queryLookupEndPoint,
		listlookupLoadingFunc: queryListLookupEndPoint,
	}

	cache := NewLookupCache(10 * time.Second, mfh.lookupQFunc, mfh.listlookupQFunc)
	defer cache.Stop()

	//local lookup, returns not found
	lookupAddr := "127.0.0.1:4161"
	v, err := cache.Lookup(lookupAddr, topic, ACC_R, true)
	assert.Nil(t, err)
	le, ok := v.(*lookupResp)
	assert.True(t, ok)
	assert.NotNil(t, le)

	v, err = cache.Lookup(lookupAddr, topic, ACC_R, true)
	assert.Nil(t, err)
	le, ok = v.(*lookupResp)
	assert.True(t, ok)
	assert.NotNil(t, le)
}

func TestLookupCacheLookupReload(t *testing.T) {
	topic := "lookup_cache_test"
	//create topic
	EnsureTopic(t, 4151, topic, -1)
	mfh := &MockQueryFuncHolder {
		expLookupCnt: 2,
		expListLookupCnt: 0,
		lookupLoadingFunc: queryLookupEndPoint,
		listlookupLoadingFunc: queryListLookupEndPoint,
	}

	expireInterval := 3 * time.Second

	cache := NewLookupCache(expireInterval, mfh.lookupQFunc, mfh.listlookupQFunc)
	defer cache.Stop()

	//local lookup, returns not found
	lookupAddr := "127.0.0.1:4161"
	v, err := cache.Lookup(lookupAddr, topic, ACC_R, true)
	assert.Nil(t, err)
	le, ok := v.(*lookupResp)
	assert.True(t, ok)
	assert.NotNil(t, le)

	v, err = cache.Lookup(lookupAddr, topic, ACC_R, true)
	assert.Nil(t, err)
	le, ok = v.(*lookupResp)
	assert.True(t, ok)
	assert.NotNil(t, le)

	time.Sleep(expireInterval)

	v, err = cache.Lookup(lookupAddr, topic, ACC_R, true)
	assert.Nil(t, err)
	le, ok = v.(*lookupResp)
	assert.True(t, ok)
	assert.NotNil(t, le)
}

