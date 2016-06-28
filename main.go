package main

import (
	"encoding/json"
	"fmt"
	"gilmour-proxy/proxy"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
)

type NodeID string

var rServiceMatch = regexp.MustCompile(`services`) // Contains "abc"
var rSlotMatch = regexp.MustCompile(`slots`)       // Contains "abc"

// All the handler functions for TCP server
func nodeOperationsHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method == "GET" {
		switch {
		case rServiceMatch.MatchString(req.URL.Path):
			getServicesHandler(w, req)
		case rSlotMatch.MatchString(req.URL.Path):
			getSlotsHandler(w, req)
		default:
			w.Write([]byte("Unknown Pattern"))
		}
	} else if req.Method == "POST" {
		switch {
		case rServiceMatch.MatchString(req.URL.Path):
			addServicesHandler(w, req)
		case rSlotMatch.MatchString(req.URL.Path):
			addSlotsHandler(w, req)
		default:
			w.Write([]byte("Unknown Pattern"))
		}
	} else if req.Method == "DELETE" {
		switch {
		case rServiceMatch.MatchString(req.URL.Path):
			removeServicesHandler(w, req)
		case rSlotMatch.MatchString(req.URL.Path):
			removeSlotsHandler(w, req)
		default:
			w.Write([]byte("Unknown Pattern"))
		}
	} else {
		http.NotFound(w, req)
	}
	return
}

func getNode(reqUrlPath string, suffixStr string) (node *proxy.Node, err error) {
	nm := proxy.GetNodeMap()
	prefixLen := len("/nodes/")
	suffixLen := strings.LastIndex(reqUrlPath, suffixStr)
	nodeId := reqUrlPath[prefixLen:suffixLen]
	node, err = nm.Get(proxy.NodeID(nodeId))
	return
}

func formatResponse(key string, value interface{}) interface{} {
	js, err := json.Marshal(map[string]interface{}{key: value})
	if err != nil {
		proxy.LogError(err)
		js = []byte(err.Error())
	}
	return js
}

func setResponseStatus(err error) string {
	status := "ok"
	if err != nil {
		status = err.Error()
	}
	return status
}

// GET /nodes/:id/services
func getServicesHandler(w http.ResponseWriter, req *http.Request) {
	reqUrlPath := req.URL.Path
	node, _ := getNode(reqUrlPath, "/services")
	response, _ := node.GetServices()
	js := formatResponse("services", response)
	w.Header().Set("Content-Type", "application/json")
	w.Write(js.([]byte))
	return
}

// GET /nodes/:id/slots
func getSlotsHandler(w http.ResponseWriter, req *http.Request) {
	reqUrlPath := req.URL.Path
	node, _ := getNode(reqUrlPath, "/slots")
	response, _ := node.GetSlots()
	js := formatResponse("slots", response)
	w.Header().Set("Content-Type", "application/json")
	w.Write(js.([]byte))
	return // will return `Slot` struct objects map
}

// DELETE /nodes/:id/services?topic=<topic>&path=<path>
func removeServicesHandler(w http.ResponseWriter, req *http.Request) {
	reqUrlPath := req.URL.Path
	node, _ := getNode(reqUrlPath, "/services")
	topic := proxy.GilmourTopic(req.URL.Query().Get("topic"))
	services, _ := node.GetServices()
	service := services[topic]
	err := node.RemoveService(topic, service)
	status := setResponseStatus(err)
	js := formatResponse("status", status)
	w.Header().Set("Content-Type", "application/json")
	w.Write(js.([]byte))
	return
}

// DELETE /nodes/:id/slots?topic=<topic>&path=<path>
func removeSlotsHandler(w http.ResponseWriter, req *http.Request) {
	reqUrlPath := req.URL.Path
	node, _ := getNode(reqUrlPath, "/slots")
	topic := req.URL.Query().Get("topic")
	path := req.URL.Query().Get("path")
	slot := proxy.Slot{
		Topic: topic,
		Path:  path,
	}
	err := node.RemoveSlot(slot)
	status := setResponseStatus(err)
	js := formatResponse("status", status)
	w.Header().Set("Content-Type", "application/json")
	w.Write(js.([]byte))
	return
}

// POST /nodes/:id/services
func addServicesHandler(w http.ResponseWriter, req *http.Request) {
	reqUrlPath := req.URL.Path
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
	}
	service := new(proxy.ServiceMap)
	err = json.Unmarshal(body, service)
	if err != nil {
		fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
	}
	node, _ := getNode(reqUrlPath, "/services")
	for topic, value := range *service {
		err := node.AddService(topic, value)
		status := setResponseStatus(err)
		js := formatResponse("status", status)
		w.Header().Set("Content-Type", "application/json")
		w.Write(js.([]byte))
	}
	return
}

// POST /nodes/:id/slots
func addSlotsHandler(w http.ResponseWriter, req *http.Request) {
	reqUrlPath := req.URL.Path
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
	}
	slot := new(proxy.Slot)
	err = json.Unmarshal(body, slot)
	if err != nil {
		fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
	}
	node, _ := getNode(reqUrlPath, "/slots")
	err = node.AddSlot(*slot)
	status := setResponseStatus(err)
	js := formatResponse("status", status)
	w.Header().Set("Content-Type", "application/json")
	w.Write(js.([]byte))
	return
}

// This function will return a `Response` struct object to the node
func createNodeHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method == "PUT" {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
		}
		nodeReq := new(proxy.NodeReq)
		if err = json.Unmarshal(body, nodeReq); err != nil {
			fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
		}
		node, err := proxy.CreateNode(nodeReq)
		if err != nil {
			fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
		}
		if err = node.Start(); err != nil {
			fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
		}
		response := node.FormatResponse()
		js, err := json.Marshal(response)
		if err != nil {
			fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
	} else {
		http.NotFound(w, req)
	}
	return
}

func main() {
	http.HandleFunc("/nodes", createNodeHandler)
	http.HandleFunc("/nodes/", nodeOperationsHandler)
	http.ListenAndServe(":8080", nil)
}
