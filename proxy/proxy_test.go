package proxy

import (
	"bytes"
	"encoding/json"
	"gilmour-proxy/proxy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"testing"

	G "gopkg.in/gilmour-libs/gilmour-e-go.v4"
)

type NodeTest struct {
	suite.Suite
	Node        *proxy.Node
	NodeRequest *proxy.NodeReq
}

func createNodeReq() (nodeReq *proxy.NodeReq) {
	nodeReq = new(proxy.NodeReq)
	nodeReq.ListenSocket = "/tmp/node1.sock"
	nodeReq.HealthCheckPath = "/health_check"
	nodeReq.Services = proxy.ServiceMap{
		proxy.GilmourTopic("echo"): proxy.Service{
			Group:   "echo",
			Path:    "/echo_handler",
			Timeout: 3,
			Data:    "",
		},
	}
	nodeReq.Slots = []proxy.Slot{
		{
			Topic:   "fib",
			Group:   "fib",
			Path:    "/fib_handler",
			Timeout: 3,
			Data:    "",
		},
	}

	return
}

func (suite *NodeTest) SetupTest() {
	var err error
	suite.NodeRequest = createNodeReq()
	engine, err := proxy.MakeGilmour("127.0.0.1:6379")
	assert.Nil(suite.T(), err, "Creating gilmour backend object should not return error")
	suite.Node, err = proxy.CreateNode(suite.NodeRequest, engine)
	assert.Nil(suite.T(), err, "Create node should not return error")
	err = suite.Node.Start()
	assert.Nil(suite.T(), err, "Starting the node should not return error")
}

func (suite *NodeTest) TearDownTest() {
	node := suite.Node
	proxy.DeleteNode(node)
}

func (suite *NodeTest) checkUnsubscribed() {
	// When node is created check if handlers (unix domain socket) for services and slots
	// are registered properly
	_, err := suite.checkSignalHandler()
	assert.Nil(suite.T(), err, "SlotHandler should not return error")

	requestResp, err := suite.checkRequestHandler()
	assert.Nil(suite.T(), err, "ServiceHandler should not return error")
	assert.Equal(suite.T(), 500, requestResp.Code, "Should be 500 as handlers are stopped")
}

func (suite *NodeTest) checkSubscribed() {
	// When node is created check if handlers (unix domain socket) for services and slots
	// are registered properly
	signalResp, err := suite.checkSignalHandler()
	assert.Nil(suite.T(), err, "SlotHandler should not return error")
	assert.Equal(suite.T(), 0, signalResp.Status, "Should be 0 if signal is sent successfully")

	requestResp, err := suite.checkRequestHandler()
	assert.Nil(suite.T(), err, "ServiceHandler should not return error")
	assert.Equal(suite.T(), 200, requestResp.Code, "Should be 200 if request is sent successfully")
}

func (suite *NodeTest) nodeStartStop() {
	node := suite.Node
	node.Stop()
	suite.checkUnsubscribed()
	node.Start()
	suite.checkSubscribed()
}

func (suite *NodeTest) getNodeServices() (services proxy.ServiceMap, err error) {
	services, err = suite.Node.GetServices()
	return
}

func (suite *NodeTest) checkServiceDoesNotExists() {
	services, _ := suite.getNodeServices()
	for topic, _ := range services {
		assertTopic := []proxy.GilmourTopic{"echo1"}
		assert.NotContains(suite.T(), assertTopic, topic, "But services should have not topic "+string(topic))
	}
}

func (suite *NodeTest) checkServiceExists() {
	services, _ := suite.getNodeServices()
	for topic, _ := range services {
		assertTopic := []proxy.GilmourTopic{"echo", "echo1"}
		assert.Contains(suite.T(), assertTopic, topic, "But services does should have topic "+string(topic))
	}
}

func (suite *NodeTest) getNodeSlots() (slots []proxy.Slot, err error) {
	slots, err = suite.Node.GetSlots()
	return
}

func (suite *NodeTest) checkSlotDoesNotExists() {
	slots, _ := suite.getNodeSlots()
	assertTopic := []string{"recursion"}
	for _, slot := range slots {
		topic := slot.Topic
		assert.NotContains(suite.T(), assertTopic, topic, "But slots should not have topic "+topic)
	}
}

func (suite *NodeTest) checkSlotExists() {
	slots, _ := suite.getNodeSlots()
	assertTopic := []string{"fib", "recursion"}
	for _, slot := range slots {
		topic := slot.Topic
		assert.Contains(suite.T(), assertTopic, topic, "But slots does should have topic "+topic)
	}
}

func (suite *NodeTest) nodeOperationsCycle() {
	node := suite.Node
	service := proxy.Service{
		Group:   "echo1",
		Path:    "/echo1_handler",
		Timeout: 3,
		Data:    "",
	}
	node.AddService(proxy.GilmourTopic("echo1"), service)
	suite.checkServiceExists()

	slot := proxy.Slot{
		Topic:   "recursion",
		Group:   "recursion",
		Path:    "/recursion_handler",
		Timeout: 3,
		Data:    "",
	}
	slot1 := proxy.Slot{
		Topic:   "recursion",
		Group:   "recursion1",
		Path:    "/recursion1_handler",
		Timeout: 3,
		Data:    "",
	}

	node.AddSlot(slot)
	suite.checkSlotExists()

	node.RemoveService(proxy.GilmourTopic("echo1"), service)
	suite.checkServiceDoesNotExists()

	// Slot with topic and handler path
	node.RemoveSlot(proxy.Slot{Topic: "recursion", Path: "recursion_handler"})
	suite.checkSlotDoesNotExists()

	// Slot with topic but no handler path
	node.AddSlot(slot)
	node.AddSlot(slot1)
	node.RemoveSlot(proxy.Slot{Topic: "recursion"})
	suite.checkSlotDoesNotExists()
}

func (suite *NodeTest) checkSlotAdded(msg string) (err error) {
	_, err = suite.Node.GetEngine().Signal("fib", G.NewMessage().SetData(msg))
	if err != nil {
		log.Println("Fib Client: error", err.Error())
	}
	return
}

func (suite *NodeTest) checkServiceAdded(msg string) (output string, err error) {
	req := suite.Node.GetEngine().NewRequest("echo")
	resp, err := req.Execute(G.NewMessage().SetData(msg))
	if err != nil {
		log.Println("Echoclient: error", err.Error())
	}

	if err := resp.Next().GetData(&output); err != nil {
		log.Println("Echoclient: error", err.Error())
	} else {
		log.Println("Echoclient: received", output)
	}
	return
}

func (suite *NodeTest) checkSignalHandler() (signalResp *proxy.SignalResponse, err error) {
	nodeId := suite.Node.GetID()
	sig := proxy.Signal{
		Topic:   "fib",
		Message: "Hello: 1",
	}
	tr := &http.Transport{
		Dial: setupConnection("/tmp/publish_socket" + nodeId[0:5] + ".sock"),
	}
	client := &http.Client{Transport: tr}
	mJson, _ := json.Marshal(sig)
	resp, err := client.Post("http://127.0.0.1/signal/"+nodeId, "application/json", bytes.NewReader(mJson))
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		panic(err)
	}
	signalResp = new(proxy.SignalResponse)
	err = json.Unmarshal(body, signalResp)
	if err != nil {
		log.Println(err)
	}
	return
}

func (suite *NodeTest) checkRequestHandler() (requestResp *proxy.RequestResponse, err error) {
	nodeId := suite.Node.GetID()
	req := proxy.Request{
		Topic:   "echo",
		Message: "Hello: 1",
	}
	tr := &http.Transport{
		Dial: setupConnection("/tmp/publish_socket" + nodeId[0:5] + ".sock"),
	}
	client := &http.Client{Transport: tr}
	mJson, _ := json.Marshal(req)
	resp, err := client.Post("http://127.0.0.1/request/"+nodeId, "application/json", bytes.NewReader(mJson))
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		panic(err)
	}
	requestResp = new(proxy.RequestResponse)
	err = json.Unmarshal(body, requestResp)
	if err != nil {
		log.Println(err)
	}
	return
}

func setupConnection(socketName string) func(string, string) (net.Conn, error) {
	return func(proto, addr string) (conn net.Conn, err error) {
		return net.Dial("unix", socketName)
	}
}

func (suite *NodeTest) checkPublishSocket() (reply string, err error) {
	NodeID := suite.Node.GetID()
	tr := &http.Transport{
		Dial: setupConnection("/tmp/publish_socket" + NodeID[0:5] + ".sock"),
	}
	client := &http.Client{Transport: tr}
	resp, err := client.Get("http://127.0.0.1/health_check/" + NodeID)
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		panic(err)
	}
	reply = string(body)
	return
}

func (suite *NodeTest) checkHealth() (err error) {
	return
}

// Check required values are present for fields
func (suite *NodeTest) assertFields() {
	node := suite.Node
	nodeReq := suite.NodeRequest
	assert.Equal(suite.T(), nodeReq.ListenSocket, node.GetListenSocket())
	assert.Equal(suite.T(), nodeReq.HealthCheckPath, node.GetHealthCheckPath())
	assert.NotNil(suite.T(), node.GetID(), "Node should have a ID")
	assert.NotNil(suite.T(), node.GetEngine(), "Node should have a *G.Gilmour struct")
	status, _ := node.GetStatus(true)
	assert.Equal(suite.T(), 1, status)
	assert.NotNil(suite.T(), node.GetPublishSocket(), "Node should have a publish socket")
}

// Tests start from here !
func (suite *NodeTest) TestCreateNode() {
	// Check node fields have values as expected
	suite.assertFields()

	// Check if node is alive
	suite.checkHealth()

	// Check unix domain socket connection can be established
	reply, err := suite.checkPublishSocket()
	if assert.NoError(suite.T(), err) {
		assert.Equal(suite.T(), "I am alive!", reply)
	}

	// Handlers are actually added for services which are registered
	// when node is created
	msg := "Hello: 1"
	resp, _ := suite.checkServiceAdded(msg)
	assert.Equal(suite.T(), "Pong Hello: 1", resp)

	// Handlers are actually added for slots which are registered
	// when node is created
	assert.Nil(suite.T(), suite.checkSlotAdded(msg), "For slot response err should be nil")

	suite.checkSubscribed()

	// Check if services and slots respond properly when added / removed
	suite.nodeOperationsCycle()

	// Stop and start node to validate if all the functions are working properly
	suite.nodeStartStop()
}

func TestSuite(t *testing.T) {
	suite.Run(t, new(NodeTest))
}
