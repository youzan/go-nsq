package nsq

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"sync/atomic"
	"time"
)

// The number of bytes for a Message.ID
const MsgIDLength = 16
const COMPRESS_RATIO_WILD_GUESS = 10

type FullMessageID [MsgIDLength]byte

// MessageID is the binary bytes message ID
type MessageID [MsgIDLength]byte

type NewMessageID uint64

func GetCompatibleMsgIDFromNew(id NewMessageID, traceID uint64) MessageID {
	var buf MessageID
	binary.BigEndian.PutUint64(buf[:8], uint64(id))
	binary.BigEndian.PutUint64(buf[8:16], uint64(traceID))
	return buf
}

func GetNewMessageID(old []byte) NewMessageID {
	return NewMessageID(binary.BigEndian.Uint64(old[:8]))
}

//ext versions
// version for message has no ext
var NoExtVer = uint8(0)

// version for message has json header ext
var JSONHeaderExtVer = uint8(4)

// Message is the fundamental data type containing
// the id, body, and metadata
type Message struct {
	ID        MessageID
	Body      []byte
	Timestamp int64
	Attempts  uint16

	NSQDAddress string
	Partition   string
	Delegate    MessageDelegate

	autoResponseDisabled int32
	responded            int32
	Offset               uint64
	RawSize              uint32

	ExtVer   uint8
	ExtBytes []byte
}

// NewMessage creates a Message, initializes some metadata,
// and returns a pointer
func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

func (m *Message) GetTraceID() uint64 {
	if len(m.ID) < 16 {
		return 0
	}
	return binary.BigEndian.Uint64(m.ID[8:16])
}

func (m *Message) GetFullMsgID() FullMessageID {
	return FullMessageID(m.ID)
}

func (m *Message) GetJsonExt() (*MsgExt, error) {
	if m.ExtVer != JSONHeaderExtVer {
		return nil, errors.New("the header is not json extention")
	}
	var jext MsgExt
	if len(m.ExtBytes) > 0 {
		err := json.Unmarshal(m.ExtBytes, &jext.Custom)
		if err != nil {
			return nil, err
		}
	}
	//fetch tag
	if tagV, exist := jext.Custom[dispatchTagExtK]; exist {
		tagStr, _ := tagV.(string)
		jext.DispatchTag = tagStr
	}
	//fetch traceID
	if traceIDV, exist := jext.Custom[traceIDExtK]; exist {
		var err error
		traceIDStr, ok := traceIDV.(string)
		if !ok {
			return nil, errors.New("traceID not string")
		}
		jext.TraceID, err = strconv.ParseUint(traceIDStr, 10, 64)
		if err != nil {
			return nil, err
		}
	}
	return &jext, nil
}

// DisableAutoResponse disables the automatic response that
// would normally be sent when a handler.HandleMessage
// returns (FIN/REQ based on the error value returned).
//
// This is useful if you want to batch, buffer, or asynchronously
// respond to messages.
func (m *Message) DisableAutoResponse() {
	atomic.StoreInt32(&m.autoResponseDisabled, 1)
}

// IsAutoResponseDisabled indicates whether or not this message
// will be responded to automatically
func (m *Message) IsAutoResponseDisabled() bool {
	return atomic.LoadInt32(&m.autoResponseDisabled) == 1
}

// HasResponded indicates whether or not this message has been responded to
func (m *Message) HasResponded() bool {
	return atomic.LoadInt32(&m.responded) == 1
}

// Finish sends a FIN command to the nsqd which
// sent this message
func (m *Message) Finish() {
	if !atomic.CompareAndSwapInt32(&m.responded, 0, 1) {
		return
	}
	m.Delegate.OnFinish(m)
}

// Touch sends a TOUCH command to the nsqd which
// sent this message
func (m *Message) Touch() {
	if m.HasResponded() {
		return
	}
	m.Delegate.OnTouch(m)
}

// Requeue sends a REQ command to the nsqd which
// sent this message, using the supplied delay.
//
// A delay of -1 will automatically calculate
// based on the number of attempts and the
// configured default_requeue_delay
func (m *Message) Requeue(delay time.Duration) {
	m.doRequeue(delay, true, false)
}

// RequeueWithoutBackoff sends a REQ command to the nsqd which
// sent this message, using the supplied delay.
//
// Notably, using this method to respond does not trigger a backoff
// event on the configured Delegate.
func (m *Message) RequeueWithoutBackoff(delay time.Duration) {
	m.doRequeue(delay, false, false)
}

// RequeueBackoffSingleConn send a REQ to the nsqd which sent this message and
// only do the backoff on the connection which send this message. This is useful while
// only a single connection need to be paused but others need continue consume.
func (m *Message) RequeueBackoffSingleConn(delay time.Duration) {
	m.doRequeue(delay, true, true)
}

func (m *Message) doRequeue(delay time.Duration, backoff bool, connOnly bool) {
	if !atomic.CompareAndSwapInt32(&m.responded, 0, 1) {
		return
	}
	m.Delegate.OnRequeue(m, delay, backoff, connOnly)
}

// WriteTo implements the WriterTo interface and serializes
// the message into the supplied producer.
//
// It is suggested that the target Writer is buffered to
// avoid performing many system calls.
func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))

	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.ID[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

// DecodeMessage deseralizes data (as []byte) and creates a new Message
func DecodeMessage(b []byte) (*Message, error) {
	if len(b) < 10+MsgIDLength {
		return nil, errors.New("not enough data to decode valid message")
	}
	var msg Message
	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])

	copy(msg.ID[:], b[10:10+MsgIDLength])
	msg.Body = b[10+MsgIDLength:]
	return &msg, nil
}

// DecodeMessage deseralizes data (as []byte) and creates a new Message
// b row bytes to decode
// ext indicate whether decode message in ext format
// disableDecompress indicate whether depress message bytes, when message is compressed
func DecodeMessageWithExt(b []byte, ext bool, disableDecompress bool) (*Message, error) {
	if len(b) < 10+MsgIDLength {
		return nil, errors.New("not enough data to decode valid message")
	}
	var msg Message
	pos := 0
	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	pos += 8
	msg.Attempts = binary.BigEndian.Uint16(b[pos : pos+2])
	pos += 2

	copy(msg.ID[:], b[pos:pos+MsgIDLength])
	pos += MsgIDLength
	if ext {
		if len(b) < pos+1 {
			return nil, errors.New("not enough data to decode valid message")
		}
		msg.ExtVer = uint8(b[pos])
		pos++
		switch msg.ExtVer {
		case 0x0:
		default:
			if len(b) < pos+2 {
				return nil, errors.New("not enough data to decode valid message")
			}
			extLen := binary.BigEndian.Uint16(b[pos : pos+2])
			pos += 2
			if len(b) < pos+int(extLen) {
				return nil, errors.New("not enough data to decode valid message")
			}
			msg.ExtBytes = b[pos : pos+int(extLen)]
			pos += int(extLen)
		}
	}
	if ext && msg.ExtVer == JSONHeaderExtVer {
		json, err := msg.GetJsonExt()
		if err != nil {
			return nil, err
		}
		if disableDecompress {
			msg.Body = b[pos:]
		} else {
			var decompressed bool
			msg.Body, decompressed, err = tryDecompress(b[pos:], json)
			if decompressed {
				removeCompressHeader(&msg, json)
			}
		}
		if err != nil {
			return nil, err
		}
	} else {
		msg.Body = b[pos:]
	}
	return &msg, nil
}

func tryDecompress(bodyMayCompressed []byte, ext *MsgExt) ([]byte, bool, error) {
	compressH := ext.Custom[NSQ_CLIENT_COMPRESS_HEADER_KEY]
	compressHStr, _ := compressH.(string)
	if compressHStr != "" {
		codecNo, err := strconv.Atoi(compressHStr)
		if err != nil {
			return nil, false, err
		}
		codec, err := GetNSQClientCompressCodeByCodecNo(codecNo)
		if err != nil {
			return nil, false, err
		}
		compressHSize := ext.Custom[NSQ_CLIENT_COMPRESS_SIZE_HEADER_KEY]
		origSizeStr, ok := compressHSize.(string)
		var originalMsgSize int
		if ok {
			originalMsgSize, err = strconv.Atoi(origSizeStr)
			if err != nil {
				return nil, false, err
			}
		} else {
			//try luck with wild guess
			originalMsgSize = len(bodyMayCompressed) * COMPRESS_RATIO_WILD_GUESS
		}
		decompressed, err := codec.Decompress(bodyMayCompressed, originalMsgSize)
		if err != nil {
			return nil, false, err
		}
		return decompressed, true, err
	} else {
		return bodyMayCompressed, false, nil
	}
}

//after a successful decompress, remove NSQ_CLIENT_COMPRESS_HEADER_KEY and NSQ_CLIENT_COMPRESS_SIZE_HEADER_KEY
//and build new json header with ext.Custom map
func removeCompressHeader(msg *Message, ext *MsgExt) error {
	//remove nsq compress codec key in header, before message process
	delete(ext.Custom, NSQ_CLIENT_COMPRESS_HEADER_KEY)
	delete(ext.Custom, NSQ_CLIENT_COMPRESS_SIZE_HEADER_KEY)
	newExtJsonBytes, err := json.Marshal(ext.Custom)
	if err != nil {
		return err
	}
	msg.ExtBytes = newExtJsonBytes
	return nil
}
