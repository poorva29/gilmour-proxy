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
	nodeReq.ListenSocket = "/tmp/listen_socket.sock"
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
	if err := proxy.DeleteNode(node); err != nil {
		log.Println(err.Error())
	}
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
	if err := node.Stop(); err != nil {
		log.Println(err.Error())
	}
	suite.checkUnsubscribed()
	if err := node.Start(); err != nil {
		log.Println(err.Error())
	}
	suite.checkSubscribed()
}

func (suite *NodeTest) getNodeServices() (services proxy.ServiceMap, err error) {
	services, err = suite.Node.GetServices()
	return
}

func (suite *NodeTest) checkServiceDoesNotExists() {
	services, err := suite.getNodeServices()
	if err != nil {
		log.Println(err.Error())
	}
	for topic := range services {
		assertTopic := []proxy.GilmourTopic{"echo1"}
		assert.NotContains(suite.T(), assertTopic, topic, "But services should have not topic "+string(topic))
	}
}

func (suite *NodeTest) checkServiceExists() {
	services, err := suite.getNodeServices()
	if err != nil {
		log.Println(err.Error())
	}
	for topic := range services {
		assertTopic := []proxy.GilmourTopic{"echo", "echo1"}
		assert.Contains(suite.T(), assertTopic, topic, "But services does should have topic "+string(topic))
	}
}

func (suite *NodeTest) getNodeSlots() (slots []proxy.Slot, err error) {
	slots, err = suite.Node.GetSlots()
	return
}

func (suite *NodeTest) checkSlotDoesNotExists() {
	slots, err := suite.getNodeSlots()
	if err != nil {
		log.Println(err.Error())
	}
	assertTopic := []string{"recursion"}
	for _, slot := range slots {
		topic := slot.Topic
		assert.NotContains(suite.T(), assertTopic, topic, "But slots should not have topic "+topic)
	}
}

func (suite *NodeTest) checkSlotExists() {
	slots, err := suite.getNodeSlots()
	if err != nil {
		log.Println(err.Error())
	}
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
	if err := node.AddService(proxy.GilmourTopic("echo1"), service); err != nil {
		log.Println(err.Error())
	}
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

	if err := node.AddSlot(slot); err != nil {
		log.Println(err.Error())
	}
	suite.checkSlotExists()

	if err := node.RemoveService(proxy.GilmourTopic("echo1"), service); err != nil {
		log.Println(err.Error())
	}
	suite.checkServiceDoesNotExists()

	// Slot with topic and handler path
	if err := node.RemoveSlot(proxy.Slot{Topic: "recursion", Path: "recursion_handler"}); err != nil {
		log.Println(err.Error())
	}
	suite.checkSlotDoesNotExists()

	// Slot with topic but no handler path
	if err := node.AddSlot(slot); err != nil {
		log.Println(err.Error())
	}
	if err := node.AddSlot(slot1); err != nil {
		log.Println(err.Error())
	}
	if err := node.RemoveSlot(proxy.Slot{Topic: "recursion"}); err != nil {
		log.Println(err.Error())
	}
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

func sendData(reqType string, NodeID string, data interface{}) (body []byte, err error) {
	reqStr := "http://127.0.0.1/" + reqType + "/" + NodeID
	tr := &http.Transport{
		Dial: setupProxyConnection("/tmp/publish_socket" + NodeID[0:5] + ".sock"),
	}
	client := &http.Client{Transport: tr}
	mJSON, err := json.Marshal(data)
	if err != nil {
		log.Println(err.Error())
	}
	resp, err := client.Post(reqStr, "application/json", bytes.NewReader(mJSON))
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		panic(err)
	}
	return
}

func (suite *NodeTest) checkSignalHandler() (signalResp *proxy.SignalResponse, err error) {
	NodeID := suite.Node.GetID()
	sig := proxy.Signal{
		Topic:   "fib",
		Message: "Hello: 1",
	}
	body, err := sendData("signal", NodeID, sig)
	if err != nil {
		log.Println(err)
	}
	signalResp = new(proxy.SignalResponse)
	err = json.Unmarshal(body, signalResp)
	if err != nil {
		log.Println(err)
	}
	return
}

func (suite *NodeTest) checkRequestHandler() (requestResp *proxy.RequestResponse, err error) {
	NodeID := suite.Node.GetID()
	req := proxy.Request{
		Topic:   "echo",
		Message: "Hello: 1",
	}
	body, err := sendData("request", NodeID, req)
	if err != nil {
		log.Println(err)
	}
	requestResp = new(proxy.RequestResponse)
	err = json.Unmarshal(body, requestResp)
	if err != nil {
		log.Println(err)
	}
	return
}

func setupProxyConnection(socketName string) func(string, string) (net.Conn, error) {
	return func(proto, addr string) (conn net.Conn, err error) {
		return net.Dial("unix", socketName)
	}
}

func (suite *NodeTest) checkPublishSocket() (reply string, err error) {
	NodeID := suite.Node.GetID()
	tr := &http.Transport{
		Dial: setupProxyConnection("/tmp/publish_socket" + NodeID[0:5] + ".sock"),
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

// Check required values are present for fields
func (suite *NodeTest) assertFields() {
	node := suite.Node
	nodeReq := suite.NodeRequest
	assert.Equal(suite.T(), nodeReq.ListenSocket, node.GetListenSocket())
	assert.Equal(suite.T(), nodeReq.HealthCheckPath, node.GetHealthCheckPath())
	assert.NotNil(suite.T(), node.GetID(), "Node should have a ID")
	assert.NotNil(suite.T(), node.GetEngine(), "Node should have a *G.Gilmour struct")
	status, err := node.GetStatus(true)
	if err != nil {
		log.Println(err.Error())
	}
	assert.Equal(suite.T(), proxy.Ok, status)
	assert.NotNil(suite.T(), node.GetPublishSocket(), "Node should have a publish socket")
}

// Tests start from here !
func (suite *NodeTest) TestCreateNode() {
	// Check node fields have values as expected
	suite.assertFields()

	// Check unix domain socket connection can be established
	reply, err := suite.checkPublishSocket()
	if assert.NoError(suite.T(), err) {
		assert.Equal(suite.T(), "I am alive!", reply)
	}

	// Handlers are actually added for services which are registered
	// when node is created
	msg := "Hello: 1"
	resp, err := suite.checkServiceAdded(msg)
	assert.Nil(suite.T(), err, "For service response err should be nil")
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
