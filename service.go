package geerpc

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

// 通过反射来获取结构体（或者说是实例类？对象）中的所有方法
type methodType struct {
	method    reflect.Method
	ArgType   reflect.Type
	ReplyType reflect.Type
	numCalls  uint64
}

func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

// 根据 ArgType 的类型创建一个新的值，并返回它
func (m *methodType) newArgv() reflect.Value {
	var argv reflect.Value
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem())
	} else {
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

// 根据 ReplyType 的类型创建一个新的值，并返回它。
// 如果 ReplyType 是映射类型或切片类型，它会创建相应的空值。
func (m *methodType) newReplyv() reflect.Value {
	replyv := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}
	return replyv
}

type service struct {
	name   string
	typ    reflect.Type
	rcvr   reflect.Value
	method map[string]*methodType
}

func newService(rcvr interface{}) *service {
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)
	s.name = reflect.Indirect(s.rcvr).Type().Name()
	s.typ = reflect.TypeOf(rcvr)
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid service name", s.name)
	}
	s.registerMethods()
	return s
}

// 通过检查方法的参数和返回值类型，筛选出符合 RPC 规范的方法并将其注册到服务中，以便后续调用
func (s *service) registerMethods() {
	s.method = make(map[string]*methodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type
		//检查入参与返回值数量
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}
		//检查返回值第一个是否是error类型
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		//检查第二和第三个参数是否为可访问的类型
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.method[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server: register %s.%s\n", s.name, method.Name)
	}

}

func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}

func (s *service) call(m *methodType, argv, replyv reflect.Value) error {
	//并发安全的原子操作 增加调用次数
	atomic.AddUint64(&m.numCalls, 1)
	//获取方法的反射对象 Func，它代表实际的方法实现
	f := m.method.Func
	//通过反射调用 Call 方法接受一个包含调用参数的切片，
	//这里传入了接收者（s.rcvr）、请求参数（argv）和响应参数（replyv）。
	//返回值是一个切片，包含该方法的返回值。
	returnValues := f.Call([]reflect.Value{s.rcvr, argv, replyv})
	//检查返回值的第一个元素（方法的返回值）。如果这个值不为 nil，则认为发生了错误。
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}
