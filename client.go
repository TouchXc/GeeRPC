package geerpc

import (
	"errors"
	"fmt"
	"geerpc/codec"
	"io"
	"sync"
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
