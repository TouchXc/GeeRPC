package geerpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"geerpc/codec"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type Call struct {
	Seq           uint64
	ServiceMethod string
	Args          interface{}
	Reply         interface{}
	Error         error
	Done          chan *Call
}

func (call *Call) done() {
	call.Done <- call
}

// 客户端的实现

type Client struct {
	cc      codec.Codec  //消息编码解码
	opt     *Option      //配置选项
	sending sync.Mutex   //加锁防止并发发送消息乱序
	header  codec.Header //请求消息头
	mu      sync.Mutex
	seq     uint64           //规定请求编号
	pending map[uint64]*Call //存储未处理完请求
	//下面两个是客户端的关闭标志位，其中：
	// closing表示用户主动关闭  shutdown表示因为某些错误而关闭的客户端
	closing  bool
	shutdown bool
}

var _ io.Closer = (*Client)(nil)

var ErrShutDown = errors.New("connect is shut down")

// 关闭连接

func (client *Client) Close() error {
	//先加锁防止并发时多次关闭
	client.mu.Lock()
	defer client.mu.Unlock()
	//这里检查之前是否关闭过客户端
	if client.closing {
		return ErrShutDown
	}
	//正式关闭客户端，把标志位设为true
	//同时调用Close方法
	client.closing = true
	return client.cc.Close()
}

//下面是Call类型相关的方法实现，调用rpc方法时，需要对调用进行存储然后进一步处理，而调用完的方法则需要移除

// registerCall：将参数 call 添加到 client.pending 中，并更新 client.seq。
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutDown
	}
	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.seq++
	return call.Seq, nil
}

// removeCall：根据 seq，从 client.pending 中移除对应的 call，并返回。
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[client.seq]
	delete(client.pending, seq)
	return call
}

// terminateCalls：服务端或客户端发生错误时调用，将 shutdown 设置为 true，
// 且将错误信息通知所有 pending 状态的 call。
func (client *Client) terminateCall(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

//实现客户端接收请求

func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		call := client.removeCall(h.Seq)
		switch { //这里有三种情况： 1.请求为空  2.请求发生了错误 3.请求正常，读取body里的内容
		case call == nil:
			err = client.cc.ReadBody(nil)
		case h.Error != "":
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		default:
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	client.terminateCall(err)
}

type clientResult struct {
	client *Client
	err    error
}

type newClientFunc func(conn net.Conn, opt *Option) (Client *Client, err error)

// NewClient 创建客户端实例
func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	//先获取消息体的编码方式，没有就直接返回错误
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error: ", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		cc:      cc,
		opt:     opt,
		seq:     1,
		pending: make(map[uint64]*Call),
	}
	//创建子协程接受请求
	go client.receive()
	return client
}

// parseOptions 把Option实现为可选参数，方便用户调用
func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

// 给Dial函数套一层外壳，添加超时处理
func dialTimeout(f newClientFunc, network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	//把原有Dial函数换成了DialTimeout函数
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()
	ch := make(chan clientResult)
	//开启协程并发创建客户端，把结果放入ch中等待读取
	go func() {
		client, err := f(conn, opt)
		ch <- clientResult{client: client, err: err}
	}()
	//如果未设置超时处理时间，直接返回
	if opt.ConnectTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}
	//监听创建客户端处理结果，超时了就返回错误  正常则返回客户端连接地址
	select {
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	case result := <-ch:
		return result.client, result.err
	}
}

// Dial 函数方便用户对服务端特定地址进行调用
func Dial(network, address string, opts ...*Option) (client *Client, err error) {
	return dialTimeout(NewClient, network, address, opts...)
}

// 下面是为客户端添加发送请求的能力
func (client *Client) send(call *Call) {
	client.sending.Lock()
	defer client.sending.Unlock()

	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}
	//组装好完整请求头
	client.header.Seq = seq
	client.header.Error = ""
	client.header.ServiceMethod = call.ServiceMethod

	if err = client.cc.Write(&client.header, call.Args); err != nil {
		//发送请求时出错
		call = client.removeCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

//下面定义了两个暴露给用户的RPC接口，Go用于异步返回call实例  Call调用Go并使其阻塞直到响应返回

func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := <-client.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	select {
	case <-ctx.Done():
		client.removeCall(call.Seq)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	case call := <-call.Done:
		return call.Error
	}
}
