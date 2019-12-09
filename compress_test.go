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
	forCompress := "Fast compression/decompression around 200~400MB/sec.Less memory usage. SnappyOutputStream uses only 32KB+ in default.JNI-based implementation to achieve comparable performance to the native C++ version.Although snappy-java uses JNI, it can be used safely with multiple class loaders (e.g. Tomcat, etc.)."
	testCompressDecompress([]byte(forCompress), CompressDecodec_LZ4, t)
}

func testCompressDecompress(forCompress []byte, codec NSQClientCompressCodec, t *testing.T) {
	compressed, err := codec.Compress(forCompress)
	assert.Nil(t, err)
	decompressed, err := codec.Decompress(compressed)
	assert.Nil(t, err)
	assert.EqualValues(t, forCompress, decompressed)
}

func TestCompressDecompressSnappyBlock(t *testing.T) {
	forCompress := "this is text for compress"
	testCompressDecompress([]byte(forCompress), CompressDecodec_SNAPPY, t)
}

func BenchmarkCompressSnappyBlock(b *testing.B) {
	forCompress := []byte("Provides stream classes for the LZ4 algorithm.The block LZ4 format which only contains the compressed data is supported by the BlockLZ4Compressor*putStream classes while the frame format is implemented by FramedLZ4Compressor*putStream. The implementation in Commons Compress is based on the specifications \"Last revised: 2015-03-26\" for the block format and version \"1.5.1 (31/03/2015)\" for the frame format.Only the frame format can be auto-detected this means you have to speficy the format explicitly if you want to read a block LZ4 stream via CompressorStreamFactory.")
	for i := 0; i < b.N; i++ {
		_, err := CompressDecodec_SNAPPY.Compress(forCompress)
		assert.Nil(b, err)
	}
}

func BenchmarkCompressSnappy(b *testing.B) {
	forCompress := []byte("Provides stream classes for the LZ4 algorithm.The block LZ4 format which only contains the compressed data is supported by the BlockLZ4Compressor*putStream classes while the frame format is implemented by FramedLZ4Compressor*putStream. The implementation in Commons Compress is based on the specifications \"Last revised: 2015-03-26\" for the block format and version \"1.5.1 (31/03/2015)\" for the frame format.Only the frame format can be auto-detected this means you have to speficy the format explicitly if you want to read a block LZ4 stream via CompressorStreamFactory.")
	for i := 0; i < b.N; i++ {
		_, err := compressSnappy(forCompress)
		assert.Nil(b, err)
	}
}

func BenchmarkCompressLZ4Block(b *testing.B) {
	forCompress := []byte("Provides stream classes for the LZ4 algorithm.The block LZ4 format which only contains the compressed data is supported by the BlockLZ4Compressor*putStream classes while the frame format is implemented by FramedLZ4Compressor*putStream. The implementation in Commons Compress is based on the specifications \"Last revised: 2015-03-26\" for the block format and version \"1.5.1 (31/03/2015)\" for the frame format.Only the frame format can be auto-detected this means you have to speficy the format explicitly if you want to read a block LZ4 stream via CompressorStreamFactory.")
	for i := 0; i < b.N; i++ {
		_, err := compressLZ4Block(forCompress)
		assert.Nil(b, err)
	}
}

func BenchmarkCompressLZ4(b *testing.B) {
	forCompress := []byte("Provides stream classes for the LZ4 algorithm.The block LZ4 format which only contains the compressed data is supported by the BlockLZ4Compressor*putStream classes while the frame format is implemented by FramedLZ4Compressor*putStream. The implementation in Commons Compress is based on the specifications \"Last revised: 2015-03-26\" for the block format and version \"1.5.1 (31/03/2015)\" for the frame format.Only the frame format can be auto-detected this means you have to speficy the format explicitly if you want to read a block LZ4 stream via CompressorStreamFactory.")
	for i := 0; i < b.N; i++ {
		_, err := compressLZ4(forCompress)
		assert.Nil(b, err)
	}
}
