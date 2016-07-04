package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	G "gopkg.in/gilmour-libs/gilmour-e-go.v4"
	"gopkg.in/gilmour-libs/gilmour-e-go.v4/backends"
)

// LogError prints error on console, returns string format of the error and raises a panic
func LogError(err error) string {
	panic(err)
	log.Println(err.Error())
	return err.Error()
}

// Status is a integer to hold status of node of the node (unavailable = 0, ok = 1 and dirty = 2)
type Status int

// Status Constant
const (
	Unavailable Status = 0 + iota
	Ok
	Dirty
)

var status = [...]string{"unavailable", "ok", "dirty"}

// String returns the English name of the status ("unavailable", "ok", ...).
func (s Status) String() string { return status[s] }

// NodeID is a string to hold node's id
type NodeID string

// GilmourTopic is a string to hold topic for signal or request
type GilmourTopic string

// implements NodeMapOperations
type nodeMap struct {
	sync.Mutex
	regNodes map[NodeID]*Node
}

func initNodeMap() (nm *nodeMap) {
	nm = new(nodeMap)
	nm.regNodes = make(map[NodeID]*Node)
	return
}

var (
	nMap = initNodeMap()
)

// NodeMapOperations is a interface to enable operation on nodeMap
type NodeMapOperations interface {
	Put(NodeID, *Node) error
	Del(NodeID) error
	Get(NodeID) (*Node, error)
}

// GetNodeMap returns nodeMap
func GetNodeMap() nodeMap {
	return *nMap
}

// Put adds node in nodeMap
func (n *nodeMap) Put(uid NodeID, node *Node) (err error) {
	n.Mutex.Lock()
	n.regNodes[uid] = node
	n.Mutex.Unlock()
	return
}

// Del removes node from nodeMap
func (n *nodeMap) Del(uid NodeID) (err error) {
	n.Mutex.Lock()
	delete(n.regNodes, uid)
	n.Mutex.Unlock()
	return
}

// Get returns node from nodeMap
func (n *nodeMap) Get(uid NodeID) (node *Node, err error) {
	n.Mutex.Lock()
	node = n.regNodes[uid]
	n.Mutex.Unlock()
	return
}

// ServiceMap is a type of GilmourTopic to string
type ServiceMap map[GilmourTopic]Service

// NodeReq is a struct of node request sent while creating a node
type NodeReq struct {
	ListenSocket    string     `json:"listen_sock"`
	HealthCheckPath string     `json:"health_check"`
	Slots           []Slot     `json:"slots"`
	Services        ServiceMap `json:"services"`
}

// Node is a struct which holds details for node and implements NodeOperations
type Node struct {
	listenSocket    string
	healthCheckPath string
	slots           []Slot
	services        ServiceMap
	status          Status
	publishSocket   net.Listener
	engine          *G.Gilmour
	id              NodeID
}

// Slot is a struct which holds details for the slot to be added / removed
type Slot struct {
	Topic        string `json:"topic"`
	Group        string `json:"group"`
	Path         string `json:"path"`
	Timeout      int    `json:"timeout"`
	Data         interface{}
	Subscription *G.Subscription
}

// Service is a struct which holds details for the service to be added / removed
type Service struct {
	Group        string `json:"group"`
	Path         string `json:"path"`
	Timeout      int    `json:"timeout"`
	Data         interface{}
	Subscription *G.Subscription
}

// CreateNodeResponse formats response for Request and signal messages coming from node
type CreateNodeResponse struct {
	ID            string   `json:"id"`
	PublishSocket net.Addr `json:"publish_socket"`
	Status        string   `json:"status"`
}

// Request is a struct for managing requests coming from node
type Request struct {
	Topic       string      `json:"topic"`
	Composition interface{} `json:"composition"`
	Message     interface{} `json:"message"`
	Timeout     int         `json:"timeout"`
}

// RequestResponse is a struct for responding to a Request
type RequestResponse struct {
	Messages map[string]interface{} `json:"messages"`
	Code     int                    `json:"code"`
	Length   int                    `json:"length"`
}

// Signal is a struct for managing signals from node
type Signal struct {
	Topic   string      `json:"topic"`
	Message interface{} `json:"message"`
}

// SignalResponse is a struct for responding to a Signal
type SignalResponse struct {
	Status int `json:"status"`
}

// NodeOperations is a interface for providing operations on a node
type NodeOperations interface {
	FormatResponse() CreateNodeResponse

	GetListenSocket() string
	GetHealthCheckpath() string
	GetID() string
	GetEngine() *G.Gilmour
	GetStatus(sync bool) (int, error)
	GetPublishSocket() net.Listener
	GetServices() (ServiceMap, error)
	GetSlots() ([]Slot, error)

	CreatePublishSocket() (net.Listener, error)
	ClosePublishSocket(net.Listener) error

	SendRequest(Request) (RequestResponse, error)
	SendSignal(Signal) (SignalResponse, error)

	RemoveService(GilmourTopic, Service) error
	RemoveServices(ServiceMap) error
	RemoveSlot(Slot) error
	RemoveSlots([]Slot) error

	AddService(GilmourTopic, Service) error
	AddServices(ServiceMap) (err error)
	AddSlot(Slot) error
	AddSlots([]Slot) (err error)

	Stop() error
	Start() error
}

// Functions for retriving node attributes

// GetListenSocket returns address of socket on which node is listening
func (node *Node) GetListenSocket() string {
	return node.listenSocket
}

// GetHealthCheckPath returns path on which health check is done
func (node *Node) GetHealthCheckPath() string {
	return node.healthCheckPath
}

// GetID returns node's ID
func (node *Node) GetID() string {
	return string(node.id)
}

// GetEngine returns gilmour backend which gilmour proxy will use
func (node *Node) GetEngine() *G.Gilmour {
	return node.engine
}

func setupConnection(conn net.Conn) func(string, string) (net.Conn, error) {
	return func(proto, addr string) (net.Conn, error) {
		return conn, nil
	}
}

// GetStatus retirves status of node by pinging and returns status of node
func (node *Node) GetStatus(sync bool) (status Status, err error) {
	conn, err := net.Dial("unix", node.listenSocket)
	if err != nil {
		log.Println(err)
		node.status = Dirty
		return node.status, err
	}
	tr := &http.Transport{
		Dial: setupConnection(conn),
	}
	client := &http.Client{Transport: tr}
	resp, err := client.Get("http://127.0.0.1/health_check")
	if err != nil {
		log.Println(err)
		node.status = Unavailable
		return node.status, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
	}
	reply := string(body)
	if reply == "I am alive!" {
		node.status = Ok
	} else {
		node.status = Unavailable

	}
	return node.status, err
}

// GetPublishSocket returns address of socket on which gilmour proxy listens for node
func (node *Node) GetPublishSocket() (conn net.Listener) {
	return node.publishSocket
}

// GetServices returns all the services which node is currently subscribed to
func (node *Node) GetServices() (services ServiceMap, err error) {
	services = node.services
	return
}

// GetSlots returns all the slots on which node is currently subscribed to
func (node *Node) GetSlots() (slots []Slot, err error) {
	slots = node.slots
	return
}

// RemoveService removes a service from the list of services which node is currently subscribed to
func (node *Node) RemoveService(topic GilmourTopic, service Service) (err error) {
	node.engine.UnsubscribeReply(string(topic), service.Subscription)
	delete(node.services, topic)
	return
}

func posByTopic(slots []Slot, topic string) int {
	for p, v := range slots {
		if v.Topic == topic {
			return p
		}
	}
	return -1
}

func posByTopicPath(slots []Slot, topic string, path string) int {
	for p, v := range slots {
		if v.Topic == topic && v.Path == "/"+path {
			return p
		}
	}
	return -1
}

// RemoveSlot removes a slot from the list of slots which node is currently subscribed to
func (node *Node) RemoveSlot(slot Slot) (err error) {
	if slot.Path != "" {
		i := posByTopicPath(node.slots, slot.Topic, slot.Path)
		if i != -1 {
			slotRemove := node.slots[i]
			node.engine.UnsubscribeSlot(slotRemove.Topic, slotRemove.Subscription)
			node.slots = append(node.slots[:i], node.slots[i+1:]...)
		}
	} else {
		for i := posByTopic(node.slots, slot.Topic); i != -1; i = posByTopic(node.slots, slot.Topic) {
			slotRemove := node.slots[i]
			node.engine.UnsubscribeSlot(slotRemove.Topic, slotRemove.Subscription)
			node.slots = append(node.slots[:i], node.slots[i+1:]...)
		}

	}
	return
}

// Used to bind the function with services
func (service Service) bindListeners() func(req *G.Request, resp *G.Message) {
	return func(req *G.Request, resp *G.Message) {
		data := service.Data
		if err := req.Data(&data); err != nil {
			log.Println(err.Error())
		}
		fmt.Println("Echoserver: received", data)
		resp.SetData(fmt.Sprintf("Pong %v", data))
	}
}

// AddService adds and subscribes a service in the existing list of services
func (node *Node) AddService(topic GilmourTopic, service Service) (err error) {
	o := G.NewHandlerOpts().SetGroup(service.Group)
	if service.Subscription, err = node.engine.ReplyTo(string(topic), service.bindListeners(), o); err != nil {
		return
	}
	node.services[topic] = service
	return
}

// AddServices adds multiple service's to the existing list of service's by subscribe them
func (node *Node) AddServices(services ServiceMap) (err error) {
	for topic, service := range services {
		if err = node.AddService(topic, service); err != nil {
			LogError(err)
			return
		}
	}
	return
}

// Used to bind the function with services
func (slot Slot) bindListeners() func(req *G.Request) {
	return func(req *G.Request) {
		data := slot.Data
		if err := req.Data(&data); err != nil {
			log.Println(err.Error())
		}
		fmt.Println("Echoserver: received", data)
	}
}

func contains(slots []Slot, slotToAdd Slot) (bool, int) {
	for pos, slot := range slots {
		if slot.Topic == slotToAdd.Topic &&
			slot.Path == slotToAdd.Path &&
			slot.Group == slotToAdd.Group {
			return true, pos
		}
	}
	return false, -1
}

// AddSlot adds and subscribes a slot in the existing list of slots
func (node *Node) AddSlot(slot Slot) (err error) {
	o := G.NewHandlerOpts().SetGroup(slot.Group)
	if slot.Subscription, err = node.engine.Slot(slot.Topic, slot.bindListeners(), o); err != nil {
		return
	}
	slotExists, pos := contains(node.slots, slot)
	if !slotExists {
		node.slots = append(node.slots, slot)
	} else {
		node.slots[pos].Subscription = slot.Subscription
	}
	return
}

// AddSlots adds multiple slots to the existing list of slot's by subscribe them
func (node *Node) AddSlots(slots []Slot) (err error) {
	for _, slot := range slots {
		if err = node.AddSlot(slot); err != nil {
			LogError(err)
			return
		}
	}
	return
}

// Functions When A Node Is Deleted
// Close socket connection , remove node and call stop
// DELETE /nodes/:id

// ClosePublishSocket closes the socket connection , thus terminating it
func ClosePublishSocket(conn net.Listener) (err error) {
	return conn.Close()
}

// Stop Exit routine. UnSubscribes Slots, removes registered health ident and triggers backend Stop
func (node *Node) Stop() (err error) {
	node.engine.Stop()
	return
}

// DeleteNode closes socket, removes entry from nMap and calls Stop
func DeleteNode(node *Node) (err error) {
	if err = ClosePublishSocket(node.publishSocket); err != nil {
		LogError(err)
		return
	}
	if err = nMap.Del(node.id); err != nil {
		LogError(err)
		return
	}
	if err = node.Stop(); err != nil {
		LogError(err)
		return
	}
	return
}

// Functions When A Node Is Added
// With POST /nodes

// FormatResponse format's response after creating node
func (node *Node) FormatResponse() (resp CreateNodeResponse) {
	resp.ID = string(node.id)
	socket := node.publishSocket
	if socket != nil {
		resp.PublishSocket = socket.Addr()
	}
	resp.Status = fmt.Sprintf("%s", node.status)
	return
}

func closeOnInterrupt(l net.Listener) (err error) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, os.Kill, syscall.SIGTERM)
	go func(c chan os.Signal) {
		// Wait for a SIGINT or SIGKILL:
		sig := <-c
		log.Printf("Caught signal %s: shutting down.", sig)
		// Stop listening (and unlink the socket if unix type):
		err = l.Close()
		// And we're done:
		os.Exit(0)
	}(sigc)
	return
}

func formatSendSingnal(status int) (sigResp SignalResponse) {
	return SignalResponse{
		Status: status,
	}
}

// SendSignal publishes message to a slot of type signal
func (node *Node) SendSignal(userSig *Signal) (sigResp SignalResponse, err error) {
	_, err = node.GetEngine().Signal(userSig.Topic, G.NewMessage().SetData(userSig.Message))
	if err != nil {
		log.Println("Fib Client: error", err.Error())
		return
	}
	sigResp = formatSendSingnal(0)
	return
}

func signalHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method == "POST" {
		nodeId := req.URL.Path[len("/signal/"):]
		node, err := nMap.Get(NodeID(nodeId))
		if err != nil {
			log.Println(err.Error())
			return
		}
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Println(err)
			panic(err)
		}
		var userSig = new(Signal)
		err = json.Unmarshal(body, userSig)
		if err != nil {
			LogError(err)
		}
		sigResp, err := node.SendSignal(userSig)
		if err != nil {
			sigResp = formatSendSingnal(1)
		}
		js, err := json.Marshal(sigResp)
		if err != nil {
			LogError(err)
		}
		w.Header().Set("Content-Type", "application/json")
		if _, err = w.Write(js); err != nil {
			log.Println(err.Error())
		}
	}
}

func formatSendRequest(outputType interface{}) (reqResp RequestResponse) {
	switch output := outputType.(type) {
	case string:
		reqResp = RequestResponse{
			Messages: map[string]interface{}{
				"result": output,
			},
			Code:   200,
			Length: len(output),
		}
	case error:
		reqResp = RequestResponse{
			Messages: map[string]interface{}{
				"result": output.Error(),
			},
			Code:   500,
			Length: len(output.Error()),
		}
	}
	return
}

// SendRequest publishes to a reply_to message of type request
func (node *Node) SendRequest(userReq *Request) (reqResp RequestResponse, err error) {
	req := node.engine.NewRequest(userReq.Topic)
	resp, err := req.Execute(G.NewMessage().SetData(userReq.Message))
	if err != nil {
		log.Println("Echoclient: error", err.Error())
		return
	}
	var output string
	if err := resp.Next().GetData(&output); err != nil {
		log.Println("Echoclient: error", err.Error())
	} else {
		log.Println("Echoclient: received", output)
	}
	reqResp = formatSendRequest(output)
	return
}

func requestHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method == "POST" {
		nodeId := req.URL.Path[len("/request/"):]
		node, err := nMap.Get(NodeID(nodeId))
		if err != nil {
			LogError(err)
			return
		}
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Println(err)
			panic(err)
		}
		var userReq = new(Request)
		err = json.Unmarshal(body, userReq)
		if err != nil {
			LogError(err)
			return
		}
		reqResp, err := node.SendRequest(userReq)
		if err != nil {
			reqResp = formatSendRequest(err)
		}
		js, err := json.Marshal(reqResp)
		if err != nil {
			LogError(err)
		}
		w.Header().Set("Content-Type", "application/json")
		if _, err = w.Write(js); err != nil {
			log.Println(err.Error())
		}
	}
}

func isAliveHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "I am %s!", "alive")
}

// NodeWatchdog checks for a status of node and depending on the status
// If dirty - calls DeleteNode
// If unavailable - calls Stop
// If ok - does nothing
// This exits when node is dirty
func NodeWatchdog(node *Node) {
	stopped := false
	for {
		<-time.After(time.Second * 10)
		status, err := node.GetStatus(true)
		if err != nil {
			log.Println(err.Error())
			return
		}
		if status == Unavailable && !stopped {
			stopped = true
			if err = node.Stop(); err != nil {
				log.Println(err.Error())
				return
			}
		} else if (status == Ok) && stopped {
			stopped = false
			if err = node.Start(); err != nil {
				log.Println(err.Error())
				return
			}
		} else if status == Dirty {
			if err = DeleteNode(node); err != nil {
				log.Println(err.Error())
				return
			}
			return
		}
		node.status = status
	}
}

// CreatePublishSocket creates a socket connection (server) on which gilmour proxy will listen to in-coming signal or request
func CreatePublishSocket(NodeID string) (l net.Listener, err error) {
	l, err = net.Listen("unix", "/tmp/publish_socket"+NodeID[0:5]+".sock")
	if err != nil {
		log.Println("listen error:", err)
		return
	}
	go func() {
		http.HandleFunc("/request/"+NodeID, requestHandler)
		http.HandleFunc("/signal/"+NodeID, signalHandler)
		http.HandleFunc("/health_check/"+NodeID, isAliveHandler)
		err = http.Serve(l, nil)
	}()
	err = closeOnInterrupt(l)
	return
}

func uniqueNodeID(strlen int) (id string) {
	rand.Seed(time.Now().UTC().UnixNano())
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, strlen)
	for i := 0; i < strlen; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}

// MakeGilmour ceates gilmour backend redis connection
func MakeGilmour(connect string) (engine *G.Gilmour, err error) {
	redis := backends.MakeRedis(connect, "")
	engine = G.Get(redis)
	return
}

// Start will Start Gilmour engine and added services of slots if any in the Node struct instance
func (node *Node) Start() (err error) {
	if node.engine == nil {
		return errors.New("Please setup backend engine")
	}
	node.engine.Start()
	if err = node.AddServices(node.services); err != nil {
		return
	}
	if err = node.AddSlots(node.slots); err != nil {
		return
	}
	return
}

// CreateNode will create a basic Node instance which will store the NodeReq data
func CreateNode(nodeReq *NodeReq, engine *G.Gilmour) (node *Node, err error) {
	node = new(Node)
	node.engine = engine
	node.id = NodeID(uniqueNodeID(50))
	node.healthCheckPath = nodeReq.HealthCheckPath
	node.listenSocket = nodeReq.ListenSocket
	node.publishSocket, err = CreatePublishSocket(string(node.id))
	node.services = make(ServiceMap)
	node.services = nodeReq.Services
	node.slots = nodeReq.Slots
	node.status, err = node.GetStatus(true)
	err = nMap.Put(node.id, node)
	return
}
