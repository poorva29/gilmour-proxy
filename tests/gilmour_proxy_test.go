package tests

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
	nodeReq.Services = map[proxy.GilmourTopic]proxy.Service{
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
	suite.Node, err = proxy.CreateNode(suite.NodeRequest)
	suite.Node.AddServices(suite.NodeRequest.Services)
	suite.Node.AddSlots(suite.NodeRequest.Slots)
	log.Println(err)
}

func (suite *NodeTest) getNodeServices() (services map[proxy.GilmourTopic]proxy.Service, err error) {
	services, err = suite.Node.GetServices()
	return
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

func (suite *NodeTest) checkSlotExists() {
	slots, _ := suite.getNodeSlots()
	assertTopic := []string{"fib", "recursion"}
	for _, slot := range slots {
		topic := slot.Topic
		assert.Contains(suite.T(), assertTopic, topic, "But slots does should have topic "+topic)
	}
}

func (suite *NodeTest) checkSubscriptionCycle() {
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
	node.AddSlot(slot)
	suite.checkSlotExists()
}

func (suite *NodeTest) addMoreSlots() (err error) {
	return
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

func (suite *NodeTest) checkSignalHandler() (err error) {
	nodeId := suite.Node.GetID()
	signalResp := new(proxy.SignalResponse)
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
	err = json.Unmarshal(body, signalResp)
	if err != nil {
		log.Println(err)
	}
	return
}

func (suite *NodeTest) checkRequestHandler() (err error) {
	nodeId := suite.Node.GetID()
	serviceResp := new(proxy.RequestResponse)
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
	err = json.Unmarshal(body, serviceResp)
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

func (suite *NodeTest) TestCreateNode() {
	msg := "Hello: 1"
	defer func() {
		publishSocket := suite.Node.GetPublishSocket()
		if publishSocket != nil {
			publishSocket.Close()
		}
	}()
	suite.assertFields()
	suite.checkHealth()
	reply, err := suite.checkPublishSocket()
	if assert.NoError(suite.T(), err) {
		assert.Equal(suite.T(), "I am alive!", reply)
	}
	resp, _ := suite.checkServiceAdded(msg)
	assert.Equal(suite.T(), "Pong Hello: 1", resp)
	assert.Nil(suite.T(), suite.checkSlotAdded(msg), "For slot response err should be nil")
	assert.Nil(suite.T(), suite.checkSignalHandler(), "SlotHandler should not return error")
	assert.Nil(suite.T(), suite.checkRequestHandler(), "ServiceHandler should not return error")
	suite.checkSubscriptionCycle()
}

func TestSuite(t *testing.T) {
	suite.Run(t, new(NodeTest))
}
