package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

// Header 定义请求头部
type Header struct {
	ServiceMethod string //服务名与方法名
	Seq           uint64 //请求序号，可以用来区分不同请求
	Error         string //错误信息
}

// Codec 抽象出消息解码编码接口（用于服务端）
type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

type NewCodecFunc func(io.ReadWriteCloser) Codec

type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json"
)

var NewCodecFuncMap map[Type]NewCodecFunc

func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}

type GobCodec struct {
	conn io.ReadWriteCloser
	buf  *bufio.Writer
	dec  *gob.Decoder //对应gob包中的Decoder
	enc  *gob.Encoder //对应gob包中的Encoder
}

// 这里用空指针去检测是不是实现了Codec这个接口
var _ Codec = (*GobCodec)(nil)

// NewGobCodec 创建GobCodec对象返回
func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(buf),
	}
}

//---------------------------------------------------------------------
//下面是利用实例类GobCodec实现接口Codec

func (c *GobCodec) ReadHeader(h *Header) error {
	return c.dec.Decode(h)
}

func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

func (c *GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		_ = c.buf.Flush()
		if err != nil {
			_ = c.Close()
			//_ = c.conn.Close()	一样的
		}
	}()
	if err = c.enc.Encode(body); err != nil {
		log.Println("rpc codec: gob error encoding body:", err)
		return err
	}
	return
}

func (c *GobCodec) Close() error {
	return c.conn.Close()
}
