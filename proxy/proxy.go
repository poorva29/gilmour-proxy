package proxy

import (
	"encoding/json"
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

// Function to log error
func LogError(err error) string {
	panic(err)
	log.Println(err)
	return err.Error()
}

type NodeID string
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

type NodeMapOperations interface {
	Put(NodeID, *Node) error
	Del(NodeID) error
	Get(NodeID) (*Node, error)
}

func GetNodeMap() nodeMap {
	return *nMap
}

// Function to modify nodeMap
func (n *nodeMap) Put(uid NodeID, node *Node) (err error) {
	n.regNodes[uid] = node
	return
}
func (n *nodeMap) Del(uid NodeID) (err error) { return }

func (n *nodeMap) Get(uid NodeID) (node *Node, err error) {
	node = n.regNodes[uid]
	return
}

type ServiceMap map[GilmourTopic]Service

type NodeReq struct {
	ListenSocket    string     `json:"listen_sock"`
	HealthCheckPath string     `json:"health_check"`
	Slots           []Slot     `json:"slots"`
	Services        ServiceMap `json:"services"`
}

// implements NodeOperations
type Node struct {
	listenSocket    string     `json:"listen_sock"`
	healthCheckPath string     `json:"health_check"`
	slots           []Slot     `json:"slots"`
	services        ServiceMap `json:"services"`
	status          string     // ** enum
	publishSocket   net.Listener
	engine          *G.Gilmour
	id              NodeID
}

type Slot struct {
	Topic        string `json:"topic"`
	Group        string `json:"group"`
	Path         string `json:"path"`
	Timeout      int    `json:"timeout"`
	Data         interface{}
	Subscription *G.Subscription
}

type Service struct {
	Group        string `json:"group"`
	Path         string `json:"path"`
	Timeout      int    `json:"timeout"`
	Data         interface{}
	Subscription *G.Subscription
}

// For Request and signal messages coming from node
type CreateNodeResponse struct {
	Id            string   `json:"id"`
	PublishSocket net.Addr `json:"publish_socket"`
	Status        string   `json:"status"`
}

type Request struct {
	Topic       string      `json:"topic"`
	Composition interface{} `json:"composition"`
	Message     interface{} `json:"message"`
	Timeout     int         `json:"timeout"`
}

type RequestResponse struct {
	Messages map[string]interface{} `json:"messages"`
	Code     int                    `json:"code"`
	Length   int                    `json:"length"`
}

type Signal struct {
	Topic   string      `json:"topic"`
	Message interface{} `json:"message"`
}

type SignalResponse struct {
	Status int `json:"status"`
}

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
	ClosePublishSocket(conn net.Listener) error

	SendRequest(Request) (RequestResponse, error)
	SendSignal(Signal) (SignalResponse, error)

	RemoveService(services ServiceMap) error
	RemoveSlot(slots []Slot) error

	AddService(GilmourTopic, Service) error
	AddServices(services ServiceMap) (err error)
	AddSlot(Slot) error
	AddSlots(slots []Slot) (err error)

	Stop() error
	Start() error
}

// Get Node Attributes
func (node *Node) GetListenSocket() string {
	return node.listenSocket
}

func (node *Node) GetHealthCheckPath() string {
	return node.healthCheckPath
}
func (node *Node) GetID() string {
	return string(node.id)
}
func (node *Node) GetEngine() *G.Gilmour {
	return node.engine
}
func (node *Node) GetStatus(sync bool) (status int, err error) {
	status = 1
	return
}
func (node *Node) GetPublishSocket() (conn net.Listener) {
	return node.publishSocket
}
func (node *Node) GetServices() (services ServiceMap, err error) {
	services = node.services
	return
}
func (node *Node) GetSlots() (slots []Slot, err error) {
	slots = node.slots
	return
}

// Used to bind the function with services
func (service Service) bindListeners() func(req *G.Request, resp *G.Message) {
	return func(req *G.Request, resp *G.Message) {
		data := service.Data
		req.Data(&data)
		fmt.Println("Echoserver: received", data)
		resp.SetData(fmt.Sprintf("Pong %v", data))
	}
}

func (node *Node) AddService(topic GilmourTopic, service Service) (err error) {
	o := G.NewHandlerOpts().SetGroup(string(service.Group))
	if service.Subscription, err = node.engine.ReplyTo(string(topic), service.bindListeners(), o); err != nil {
		return
	}
	node.services[topic] = service
	return
}

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
		req.Data(&data)
		fmt.Println("Echoserver: received", data)
	}
}

func (node *Node) AddSlot(slot Slot) (err error) {
	o := G.NewHandlerOpts().SetGroup(slot.Group)
	if slot.Subscription, err = node.engine.Slot(slot.Topic, slot.bindListeners(), o); err != nil {
		return
	}
	node.slots = append(node.slots, slot)
	return
}

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

func ClosePublishSocket(conn net.Listener) (err error) { return }

func DeleteNode(*Node) (err error) { return }

func deleteNodeHandler(w http.ResponseWriter, req *http.Request) { return }

// Functions When A Node Is Added
// With POST /nodes

func (node *Node) FormatResponse() (resp CreateNodeResponse) {
	resp.Id = string(node.id)
	socket := node.publishSocket
	if socket != nil {
		resp.PublishSocket = socket.Addr()
	}
	resp.Status = node.status
	return
}

func NodeWatchdog(*Node) {}

func closeOnInterrupt(l net.Listener) (err error) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, os.Kill, syscall.SIGTERM)
	go func(c chan os.Signal) {
		// Wait for a SIGINT or SIGKILL:
		sig := <-c
		log.Printf("Caught signal %s: shutting down.", sig)
		// Stop listening (and unlink the socket if unix type):
		l.Close()
		// And we're done:
		os.Exit(0)
	}(sigc)
	return
}

// Functions related to publish socket
func (node *Node) SendSignal(userSig *Signal) (sigResp SignalResponse, err error) {
	_, err = node.GetEngine().Signal(userSig.Topic, G.NewMessage().SetData(userSig.Message))
	if err != nil {
		log.Println("Fib Client: error", err.Error())
	}
	sigResp = SignalResponse{
		Status: 1,
	}
	return
}

func signalHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method == "POST" {
		nodeId := req.URL.Path[len("/signal/"):]
		node, _ := nMap.Get(NodeID(nodeId))
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Println(err)
			panic(err)
		}
		var userSig = new(Signal)
		err = json.Unmarshal(body, userSig)
		if err != nil {
			panic(err)
			log.Println(err)
		}
		sigResp, err := node.SendSignal(userSig)
		js, err := json.Marshal(sigResp)
		if err != nil {
			panic(err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
	}
}

func (node *Node) SendRequest(userReq *Request) (reqResp RequestResponse, err error) {
	req := node.engine.NewRequest(userReq.Topic)
	resp, err := req.Execute(G.NewMessage().SetData(userReq.Message))
	if err != nil {
		log.Println("Echoclient: error", err.Error())
	}

	var output string
	if err := resp.Next().GetData(&output); err != nil {
		log.Println("Echoclient: error", err.Error())
	} else {
		log.Println("Echoclient: received", output)
	}
	reqResp = RequestResponse{
		Messages: map[string]interface{}{
			"result": output,
		},
		Length: len(output),
	}
	return
}

func requestHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method == "POST" {
		nodeId := req.URL.Path[len("/request/"):]
		node, _ := nMap.Get(NodeID(nodeId))
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Println(err)
			panic(err)
		}
		var userReq = new(Request)
		err = json.Unmarshal(body, userReq)
		if err != nil {
			panic(err)
			log.Println(err)
		}
		reqResp, err := node.SendRequest(userReq)
		js, err := json.Marshal(reqResp)
		if err != nil {
			panic(err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
	}
}

func isAliveHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "I am %s!", "alive")
}

func CreatePublishSocket(NodeID string) (l net.Listener, err error) {
	l, err = net.Listen("unix", "/tmp/publish_socket"+NodeID[0:5]+".sock")
	if err != nil {
		log.Println("listen error:", err)
		return
	}
	go func() {
		for {
			http.HandleFunc("/request/"+NodeID, requestHandler)
			http.HandleFunc("/signal/"+NodeID, signalHandler)
			http.HandleFunc("/health_check/"+NodeID, isAliveHandler)
			err = http.Serve(l, nil)
		}
	}()
	closeOnInterrupt(l)
	return
}

func uniqueNodeId(strlen int) (id string) {
	rand.Seed(time.Now().UTC().UnixNano())
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, strlen)
	for i := 0; i < strlen; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}

func makeGilmour(connect string) (engine *G.Gilmour, err error) {
	redis := backends.MakeRedis(connect, "")
	engine = G.Get(redis)
	return
}

func CreateNode(nodeReq *NodeReq) (node *Node, err error) {
	node = new(Node)
	engine, _ := makeGilmour("127.0.0.1:6379")
	node.engine = engine
	node.id = NodeID(uniqueNodeId(50))
	node.healthCheckPath = nodeReq.HealthCheckPath
	node.listenSocket = nodeReq.ListenSocket
	node.publishSocket, err = CreatePublishSocket(string(node.id))
	node.services = make(ServiceMap)
	nMap.Put(node.id, node)
	engine.Start()
	return
}
