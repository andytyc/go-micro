// Package http provides a http based message broker
package broker

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
	"go-micro.dev/v4/codec/json"
	merr "go-micro.dev/v4/errors"
	"go-micro.dev/v4/registry"
	"go-micro.dev/v4/registry/cache"
	maddr "go-micro.dev/v4/util/addr"
	mnet "go-micro.dev/v4/util/net"
	mls "go-micro.dev/v4/util/tls"
	"golang.org/x/net/http2"
)

// HTTP Broker is a point to point async broker // HTTP Broker 是点对点异步代理
type httpBroker struct {
	id      string
	address string
	opts    Options

	// http.Serve 时需要
	mux *http.ServeMux

	c *http.Client
	r registry.Registry

	sync.RWMutex
	// 订阅者名单
	subscribers map[string][]*httpSubscriber
	running     bool

	// 接收退出信号
	exit chan chan error

	// offline message inbox
	mtx sync.RWMutex

	// 缓存等待发布的消息
	inbox map[string][][]byte
}

type httpSubscriber struct {
	opts  SubscribeOptions
	id    string
	topic string
	fn    Handler
	svc   *registry.Service
	hb    *httpBroker
}

// httpEvent broker的异步事件，给订阅者发生这个实体，并阻塞记录对方返回的err
type httpEvent struct {
	m   *Message
	t   string
	err error
}

var (
	DefaultPath      = "/"
	DefaultAddress   = "127.0.0.1:0"
	serviceName      = "micro.http.broker"
	broadcastVersion = "ff.http.broadcast"
	registerTTL      = time.Minute
	registerInterval = time.Second * 30
)

func init() {
	rand.Seed(time.Now().Unix())
}

func newTransport(config *tls.Config) *http.Transport {
	if config == nil {
		config = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	dialTLS := func(network string, addr string) (net.Conn, error) {
		return tls.Dial(network, addr, config)
	}

	t := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
		DialTLS:             dialTLS,
	}
	runtime.SetFinalizer(&t, func(tr **http.Transport) {
		(*tr).CloseIdleConnections()
	})

	// setup http2
	http2.ConfigureTransport(t)

	return t
}

func newHttpBroker(opts ...Option) Broker {
	options := Options{
		Codec:    json.Marshaler{},
		Context:  context.TODO(),
		Registry: registry.DefaultRegistry,
	}

	for _, o := range opts {
		o(&options)
	}

	// set address
	addr := DefaultAddress

	if len(options.Addrs) > 0 && len(options.Addrs[0]) > 0 {
		addr = options.Addrs[0]
	}

	h := &httpBroker{
		id:          uuid.New().String(),
		address:     addr,
		opts:        options,
		r:           options.Registry,
		c:           &http.Client{Transport: newTransport(options.TLSConfig)},
		subscribers: make(map[string][]*httpSubscriber),
		exit:        make(chan chan error),
		mux:         http.NewServeMux(),
		inbox:       make(map[string][][]byte),
	}

	// specify the message handler
	h.mux.Handle(DefaultPath, h)

	// get optional handlers
	if h.opts.Context != nil {
		handlers, ok := h.opts.Context.Value("http_handlers").(map[string]http.Handler)
		if ok {
			for pattern, handler := range handlers {
				h.mux.Handle(pattern, handler)
			}
		}
	}

	return h
}

func (h *httpEvent) Ack() error {
	return nil
}

func (h *httpEvent) Error() error {
	return h.err
}

func (h *httpEvent) Message() *Message {
	return h.m
}

func (h *httpEvent) Topic() string {
	return h.t
}

func (h *httpSubscriber) Options() SubscribeOptions {
	return h.opts
}

func (h *httpSubscriber) Topic() string {
	return h.topic
}

func (h *httpSubscriber) Unsubscribe() error {
	return h.hb.unsubscribe(h)
}

// saveMessage 缓存等待发布的消息
func (h *httpBroker) saveMessage(topic string, msg []byte) {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	// get messages
	c := h.inbox[topic]

	// save message
	c = append(c, msg)

	// max length 64
	if len(c) > 64 {
		c = c[:64]
	}

	// save inbox
	h.inbox[topic] = c
}

// getMessage 获取等待发布的消息
func (h *httpBroker) getMessage(topic string, num int) [][]byte {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	// get messages
	c, ok := h.inbox[topic]
	if !ok {
		return nil
	}

	// more message than requests
	if len(c) >= num {
		msg := c[:num]
		h.inbox[topic] = c[num:]
		return msg
	}

	// reset inbox
	h.inbox[topic] = nil

	// return all messages
	return c
}

func (h *httpBroker) subscribe(s *httpSubscriber) error {
	h.Lock()
	defer h.Unlock()

	if err := h.r.Register(s.svc, registry.RegisterTTL(registerTTL)); err != nil {
		return err
	}

	h.subscribers[s.topic] = append(h.subscribers[s.topic], s)
	return nil
}

func (h *httpBroker) unsubscribe(s *httpSubscriber) error {
	h.Lock()
	defer h.Unlock()

	//nolint:prealloc
	var subscribers []*httpSubscriber

	// look for subscriber
	for _, sub := range h.subscribers[s.topic] {
		// deregister and skip forward
		if sub == s {
			_ = h.r.Deregister(sub.svc)
			continue
		}
		// keep subscriber
		subscribers = append(subscribers, sub)
	}

	// set subscribers
	h.subscribers[s.topic] = subscribers

	return nil
}

func (h *httpBroker) run(l net.Listener) {
	t := time.NewTicker(registerInterval)
	defer t.Stop()

	for {
		select {
		// heartbeat for each subscriber 对每个订阅者进行心跳
		case <-t.C:
			h.RLock()
			for _, subs := range h.subscribers {
				for _, sub := range subs {
					_ = h.r.Register(sub.svc, registry.RegisterTTL(registerTTL))
				}
			}
			h.RUnlock()
		// received exit signal 监听收到退出信号
		case ch := <-h.exit:
			// ch 将停止是否顺利成功，将错误发送回触发方
			ch <- l.Close()
			h.RLock()
			for _, subs := range h.subscribers {
				for _, sub := range subs {
					_ = h.r.Deregister(sub.svc)
				}
			}
			h.RUnlock()
			return
		}
	}
}

func (h *httpBroker) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		err := merr.BadRequest("go.micro.broker", "Method not allowed")
		http.Error(w, err.Error(), http.StatusMethodNotAllowed)
		return
	}
	defer req.Body.Close()

	req.ParseForm()

	b, err := io.ReadAll(req.Body)
	if err != nil {
		errr := merr.InternalServerError("go.micro.broker", "Error reading request body: %v", err)
		w.WriteHeader(500)
		w.Write([]byte(errr.Error()))
		return
	}

	var m *Message
	if err = h.opts.Codec.Unmarshal(b, &m); err != nil {
		errr := merr.InternalServerError("go.micro.broker", "Error parsing request body: %v", err)
		w.WriteHeader(500)
		w.Write([]byte(errr.Error()))
		return
	}

	topic := m.Header["Micro-Topic"]
	//delete(m.Header, ":topic")

	if len(topic) == 0 {
		errr := merr.InternalServerError("go.micro.broker", "Topic not found")
		w.WriteHeader(500)
		w.Write([]byte(errr.Error()))
		return
	}

	p := &httpEvent{m: m, t: topic}
	id := req.Form.Get("id") // id 消息交互的唯一ID, 比如:cli发生 id=2 的消息, srv订阅 id=2 的消息

	//nolint:prealloc
	var subs []Handler

	h.RLock()
	for _, subscriber := range h.subscribers[topic] {
		// 遍历所有的订阅
		if id != subscriber.id {
			continue
		}
		subs = append(subs, subscriber.fn)
	}
	h.RUnlock()

	// execute the handler
	for _, fn := range subs {
		// fn 执行事件
		p.err = fn(p)
	}
}

func (h *httpBroker) Address() string {
	h.RLock()
	defer h.RUnlock()
	return h.address
}

// Connect 开启连接
func (h *httpBroker) Connect() error {
	h.RLock()
	if h.running {
		h.RUnlock()
		return nil
	}
	h.RUnlock()

	h.Lock()
	defer h.Unlock()

	var l net.Listener
	var err error

	if h.opts.Secure || h.opts.TLSConfig != nil {
		config := h.opts.TLSConfig

		fn := func(addr string) (net.Listener, error) {
			if config == nil {
				// TLS配置为空时，采用默认TLS配置
				hosts := []string{addr}

				// check if its a valid host:port 核查地址是否有效
				if host, _, err := net.SplitHostPort(addr); err == nil {
					if len(host) == 0 {
						hosts = maddr.IPs()
					} else {
						hosts = []string{host}
					}
				}

				// generate a certificate 生成tls证书
				cert, err := mls.Certificate(hosts...)
				if err != nil {
					return nil, err
				}
				config = &tls.Config{Certificates: []tls.Certificate{cert}}
			}
			// 创建 tls 监听对象:tcp ,下边的 http.Serve 会加上 http 协议
			return tls.Listen("tcp", addr, config)
		}

		l, err = mnet.Listen(h.address, fn)
	} else {
		fn := func(addr string) (net.Listener, error) {
			// 创建 no tls 监听对象:tcp ,下边的 http.Serve 会加上 http 协议
			return net.Listen("tcp", addr)
		}

		l, err = mnet.Listen(h.address, fn)
	}

	if err != nil {
		return err
	}

	addr := h.address
	h.address = l.Addr().String()

	// Serve 接收 Http协议的连接
	go http.Serve(l, h.mux)
	go func() {
		// 启动
		h.run(l)
		h.Lock()
		h.opts.Addrs = []string{addr}
		h.address = addr
		h.Unlock()
	}()

	// get registry
	reg := h.opts.Registry
	if reg == nil {
		reg = registry.DefaultRegistry
	}
	// set cache
	h.r = cache.New(reg)

	// set running
	h.running = true
	return nil
}

// Disconnect 断开连接
func (h *httpBroker) Disconnect() error {
	h.RLock()
	if !h.running {
		h.RUnlock()
		return nil
	}
	h.RUnlock()

	h.Lock()
	defer h.Unlock()

	// stop cache
	rc, ok := h.r.(cache.Cache)
	if ok {
		rc.Stop()
	}

	// exit and return err
	ch := make(chan error)
	h.exit <- ch
	err := <-ch

	// set not running
	h.running = false
	return err
}

// Init 初始化操作
func (h *httpBroker) Init(opts ...Option) error {
	h.RLock()
	if h.running {
		h.RUnlock()
		return errors.New("cannot init while connected")
	}
	h.RUnlock()

	h.Lock()
	defer h.Unlock()

	for _, o := range opts {
		o(&h.opts)
	}

	if len(h.opts.Addrs) > 0 && len(h.opts.Addrs[0]) > 0 {
		h.address = h.opts.Addrs[0]
	}

	if len(h.id) == 0 {
		h.id = "go.micro.http.broker-" + uuid.New().String()
	}

	// get registry
	reg := h.opts.Registry
	if reg == nil {
		reg = registry.DefaultRegistry
	}

	// get cache
	if rc, ok := h.r.(cache.Cache); ok {
		rc.Stop()
	}

	// set registry
	h.r = cache.New(reg)

	// reconfigure tls config
	if c := h.opts.TLSConfig; c != nil {
		h.c = &http.Client{
			Transport: newTransport(c),
		}
	}

	return nil
}

func (h *httpBroker) Options() Options {
	return h.opts
}

// Publish 发布消息
func (h *httpBroker) Publish(topic string, msg *Message, opts ...PublishOption) error {
	// create the message first // 新建一个消息实例
	m := &Message{
		Header: make(map[string]string),
		Body:   msg.Body,
	}

	for k, v := range msg.Header {
		m.Header[k] = v
	}

	m.Header["Micro-Topic"] = topic

	// encode the message 编码序列化
	b, err := h.opts.Codec.Marshal(m)
	if err != nil {
		return err
	}

	// save the message
	h.saveMessage(topic, b)

	// now attempt to get the service // 现在尝试获取服务, s 这个服务就是需要发布的目标:broker服务
	h.RLock()
	s, err := h.r.GetService(serviceName)
	if err != nil {
		h.RUnlock()
		return err
	}
	h.RUnlock()

	/*
		node 节点是在 broker注册的服务 s 上的那些订阅者
	*/

	// pub 给这个 node 节点发布消息的方法
	pub := func(node *registry.Node, t string, b []byte) error {
		scheme := "http"

		// check if secure is added in metadata 核查是否设置了TLS需求
		if node.Metadata["secure"] == "true" {
			scheme = "https"
		}

		// vals.Encode() => xxx=yy&bb=cc
		vals := url.Values{}
		vals.Add("id", node.Id)

		uri := fmt.Sprintf("%s://%s%s?%s", scheme, node.Address, DefaultPath, vals.Encode())
		r, err := h.c.Post(uri, "application/json", bytes.NewReader(b))
		if err != nil {
			return err
		}

		// discard response body 丢弃响应体,并关闭
		//
		// io.Discard 是一个 Writer，所有 Write 调用都在其上成功，无需执行任何操作。
		// io.Discard discard 实现 ReaderFrom 作为优化，所以 Copy to io.Discard 可以避免做不必要的工作。
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
		return nil
	}

	// srv 给这个 s 服务发布消息的方法
	srv := func(s []*registry.Service, b []byte) {
		for _, service := range s {
			var nodes []*registry.Node

			for _, node := range service.Nodes {
				// only use nodes tagged with broker http // 只使用带有代理 http 标记的节点
				if node.Metadata["broker"] != "http" {
					continue
				}

				// look for nodes for the topic // 符合topic的节点
				if node.Metadata["topic"] != topic {
					continue
				}

				nodes = append(nodes, node)
			}

			// only process if we have nodes // 只有当我们有节点时才处理
			if len(nodes) == 0 {
				continue
			}

			switch service.Version {
			// broadcast version means broadcast to all nodes // 广播版本表示广播到所有节点
			case broadcastVersion:
				var success bool

				// publish to all nodes // 发布给所有节点
				for _, node := range nodes {
					// publish async // 异步发布
					if err := pub(node, topic, b); err == nil {
						success = true
					}
				}

				// save if it failed to publish at least once // 如果一个都没发布成功，则继续缓存消息, 等待下次发送 ps:广播，这里认为有一个成功就是成功
				if !success {
					h.saveMessage(topic, b)
				}
			default:
				// select node to publish to // 随机一个节点进行发布
				node := nodes[rand.Int()%len(nodes)]

				// publish async to one node
				if err := pub(node, topic, b); err != nil {
					// if failed save it // 失败的话，继续缓存消息, 等待下次发送
					h.saveMessage(topic, b)
				}
			}
		}
	}

	// do the rest async // 做接下来的异步操作
	go func() {
		// get a third of the backlog
		messages := h.getMessage(topic, 8)
		delay := (len(messages) > 1)
		//
		// delay = true 表示有消息积压
		// ps: 正常是缓存区有一个消息写入马上就会读出，若有缓存消息多于1个，说明有积压
		// 积压原因很多，比如：发送消息很多失败的，所以失败的消息积压在当前map中，失败原因：对方服务挂了无响应，处理缓慢超时等

		// publish all the messages // 发布消息
		for _, msg := range messages {
			// serialize here // 序列化消息并进行发送
			srv(s, msg)

			// sending a backlog of messages // 发送积压的消息,间隔100ms
			if delay {
				time.Sleep(time.Millisecond * 100)
			}
		}
	}()

	return nil
}

// Subscribe 订阅消息
func (h *httpBroker) Subscribe(topic string, handler Handler, opts ...SubscribeOption) (Subscriber, error) {
	var err error
	var host, port string
	options := NewSubscribeOptions(opts...)

	// parse address for host, port
	host, port, err = net.SplitHostPort(h.Address())
	if err != nil {
		return nil, err
	}

	addr, err := maddr.Extract(host)
	if err != nil {
		return nil, err
	}

	var secure bool

	if h.opts.Secure || h.opts.TLSConfig != nil {
		secure = true
	}

	// register service 注册服务节点 node
	node := &registry.Node{
		Id:      topic + "-" + h.id,
		Address: mnet.HostPort(addr, port),
		Metadata: map[string]string{
			"secure": fmt.Sprintf("%t", secure),
			"broker": "http",
			"topic":  topic,
		},
	}

	// check for queue group or broadcast queue // 检查队列组或广播队列
	version := options.Queue
	if len(version) == 0 {
		version = broadcastVersion // 默认广播队列
	}

	// register service 注册服务
	service := &registry.Service{
		Name:    serviceName,
		Version: version,
		Nodes:   []*registry.Node{node}, // 注册节点
	}

	// generate subscriber 生成订阅者
	subscriber := &httpSubscriber{
		opts:  options,
		hb:    h,
		id:    node.Id,
		topic: topic,
		fn:    handler,
		svc:   service,
	}

	// subscribe now
	if err := h.subscribe(subscriber); err != nil {
		return nil, err
	}

	// return the subscriber
	return subscriber, nil
}

func (h *httpBroker) String() string {
	return "http"
}

// NewBroker returns a new http broker
func NewBroker(opts ...Option) Broker {
	return newHttpBroker(opts...)
}
