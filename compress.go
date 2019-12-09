package nsq

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/pierrec/lz4"

	"github.com/golang/snappy"
)

const NSQ_CLIENT_COMPRESS_HEADER_KEY = "nsq.client.compress"

var CompressDecodec_NOT_SPECIFIED = &NSQClientCompressCodecImpl{
	"not_specified",
	0,
}
var CompressDecodec_GZIP = &NSQClientCompressCodecImpl{
	"gzip",
	1,
}
var CompressDecodec_SNAPPY = &NSQClientCompressCodecImpl{
	"snappy",
	2,
}
var CompressDecodec_LZ4 = &NSQClientCompressCodecImpl{
	"lz4",
	3,
}

const htSize = 1 << 16

type NSQClientCompressCodecImpl struct {
	CodecName string
	codecNo   int
}

type NSQClientCompressCodec interface {
	GetCodecNo() int
	Compress(forCompress []byte) ([]byte, error)
	Decompress(forDecompress []byte) ([]byte, error)
}

func GetNSQClientCompressCodec(codecName string) (NSQClientCompressCodec, error) {
	switch codecName {
	case CompressDecodec_NOT_SPECIFIED.CodecName:
		return CompressDecodec_NOT_SPECIFIED, nil
	case CompressDecodec_GZIP.CodecName:
		return CompressDecodec_GZIP, nil
	case CompressDecodec_SNAPPY.CodecName:
		return CompressDecodec_SNAPPY, nil
	case CompressDecodec_LZ4.CodecName:
		return CompressDecodec_LZ4, nil
	default:
		return nil, errors.New(fmt.Sprintf("unsupported client compress codec: %s", codecName))
	}
}

func GetNSQClientCompressCodeByCodecNo(codecNo int) (NSQClientCompressCodec, error) {
	switch codecNo {
	case 0:
		return CompressDecodec_NOT_SPECIFIED, nil
	case 1:
		return CompressDecodec_GZIP, nil
	case 2:
		return CompressDecodec_SNAPPY, nil
	case 3:
		return CompressDecodec_LZ4, nil
	default:
		return nil, errors.New("unsupported client compress codec")
	}
}

func (c *NSQClientCompressCodecImpl) GetCodecNo() int {
	return c.codecNo
}

func (c *NSQClientCompressCodecImpl) Compress(forCompress []byte) ([]byte, error) {
	switch c.CodecName {
	case CompressDecodec_SNAPPY.CodecName:
		return compressSnappyBlock(forCompress)
	case CompressDecodec_GZIP.CodecName:
		return compressGZIP(forCompress)
	case CompressDecodec_LZ4.CodecName:
		return compressLZ4Block(forCompress)
	default:
		return forCompress, nil
	}
}

func (c *NSQClientCompressCodecImpl) Decompress(forDecompress []byte) ([]byte, error) {
	switch c.CodecName {
	case CompressDecodec_SNAPPY.CodecName:
		return decompressSnappyBlock(forDecompress)
	case CompressDecodec_GZIP.CodecName:
		return decompressGZIP(forDecompress)
	case CompressDecodec_LZ4.CodecName:
		return decompressLZ4Block(forDecompress)
	default:
		return forDecompress, nil
	}
}

func decompressLZ4(forDecompress []byte) ([]byte, error) {
	b := bytes.NewReader(forDecompress)
	zr := lz4.NewReader(b)
	r, err := ioutil.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func decompressLZ4Block(forDecompress []byte) ([]byte, error) {
	dest := make([]byte, lz4.CompressBlockBound(len(forDecompress)))
	decompressed, err := lz4.UncompressBlock(forDecompress, dest)
	if err != nil {
		return nil, err
	}
	return dest[:decompressed], nil
}

func compressLZ4(forCompress []byte) ([]byte, error) {
	var b bytes.Buffer
	w := lz4.NewWriter(&b)

	_, err := w.Write(forCompress)
	if err != nil {
		return nil, err
	}

	if err = w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

var ErrNotCompressible = errors.New("bytes is not compressible")

func compressLZ4Block(forCompress []byte) ([]byte, error) {
	dest := make([]byte, len(forCompress))
	var hashTab [htSize]int
	compressed, err := lz4.CompressBlock(forCompress, dest, hashTab[:])
	if compressed == 0 {
		return nil, ErrNotCompressible
	}
	return dest[:compressed], err
}

func decompressGZIP(forDecompress []byte) ([]byte, error) {
	b := bytes.NewReader(forDecompress)
	zr, err := gzip.NewReader(b)
	if err != nil {
		return nil, err
	}
	r, err := ioutil.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	if err = zr.Close(); err != nil {
		return nil, err
	}
	return r, nil
}

func compressGZIP(forCompress []byte) ([]byte, error) {
	var b bytes.Buffer
	zw := gzip.NewWriter(&b)

	_, err := zw.Write(forCompress)
	if err != nil {
		return nil, err
	}

	if err = zw.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

//snappy decompress
func decompressSnappy(forDecompress []byte) ([]byte, error) {
	b := bytes.NewReader(forDecompress)
	r := snappy.NewReader(b)
	return ioutil.ReadAll(r)
}

//snappy compress
func compressSnappy(forCompress []byte) ([]byte, error) {
	var b bytes.Buffer
	w := snappy.NewBufferedWriter(&b)
	_, err := w.Write(forCompress)
	if err != nil {
		return nil, err
	}
	if err = w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func compressSnappyBlock(forCompress []byte) ([]byte, error) {
	return snappy.Encode(nil, forCompress), nil
}

func decompressSnappyBlock(forDecompress []byte) ([]byte, error) {
	return snappy.Decode(nil, forDecompress)
}

//get NSQ client compress codec according to paasin codec name string
func GetCompressCodec(codecName string) NSQClientCompressCodec {
	switch codecName {
	case CompressDecodec_GZIP.CodecName:
		return CompressDecodec_GZIP
	case CompressDecodec_SNAPPY.CodecName:
		return CompressDecodec_SNAPPY
	default:
		return CompressDecodec_NOT_SPECIFIED
	}
}
