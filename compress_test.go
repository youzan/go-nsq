package nsq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompressDecompressSnappy(t *testing.T) {
	forCompress := "this is text for compress"
	testCompressDecompress([]byte(forCompress), CompressDecodec_SNAPPY, t)
}

func TestCompressDecompressGZIP(t *testing.T) {
	forCompress := "this is text for compress"
	testCompressDecompress([]byte(forCompress), CompressDecodec_GZIP, t)
}

func TestCompressDecompressLZ4(t *testing.T) {
	forCompress := "this is text for compress"
	testCompressDecompress([]byte(forCompress), CompressDecodec_LZ4, t)
}

func testCompressDecompress(forCompress []byte, codec NSQClientCompressCodec, t *testing.T) {
	compressed, err := codec.Compress(forCompress)
	assert.Nil(t, err)
	decompressed, err := codec.Decompress(compressed)
	assert.Nil(t, err)
	assert.EqualValues(t, forCompress, decompressed)
}
