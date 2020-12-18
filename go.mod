module github.com/youzan/go-nsq

go 1.13

require (
	github.com/bitly/go-simplejson v0.5.0 // indirect
	github.com/frankban/quicktest v1.7.2 // indirect
	github.com/golang/snappy v0.0.1
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/pierrec/lz4 v2.4.1+incompatible
	github.com/spaolacci/murmur3 v1.1.0
	github.com/stretchr/testify v1.4.0
	github.com/ztrue/shutdown v0.1.1
	gitlab.qima-inc.com/paas/yz-go-libs v0.0.0-20200306015615-b7cf4c8f27a5
)

replace (
	gitlab.qima-inc.com/paas/yz-go-libs => ../../../gitlab.qima-inc.com/paas/yz-go-libs
)
