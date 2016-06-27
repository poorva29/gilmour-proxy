package main

import (
	"encoding/json"
	"fmt"
	"gilmour-proxy/proxy"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strings"
)

type NodeID string

// Function to log error
func logError(err error) string {
	panic(err)
	log.Println(err)
	return err.Error()
}

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

// GET /nodes/:id/services
func getServicesHandler(w http.ResponseWriter, req *http.Request) {
	reqUrlPath := req.URL.Path
	node, _ := getNode(reqUrlPath, "/services")
	response, _ := node.GetServices()
	js, err := json.Marshal(response)
	if err != nil {
		fmt.Fprintf(w, "Error : %s!", logError(err))
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

// GET /nodes/:id/slots
func getSlotsHandler(w http.ResponseWriter, req *http.Request) {
	reqUrlPath := req.URL.Path
	node, _ := getNode(reqUrlPath, "/slots")
	response, _ := node.GetSlots()
	js, err := json.Marshal(response)
	if err != nil {
		fmt.Fprintf(w, "Error : %s!", logError(err))
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
	return // will return `Slot` struct objects map
}

// POST /request/:id
func sendRequestHandler(w http.ResponseWriter, req *http.Request) {
	return // will return a `RequestResponse` object
}

// POST /signal/:id
func sendSignalHandler(w http.ResponseWriter, req *http.Request) {
	return // will return a `SignalResponse` struct object
}

// DELETE /nodes/:id/services?topic=<topic>&path=<path>
func removeServicesHandler(w http.ResponseWriter, req *http.Request) { return }

// DELETE /nodes/:id/slots?topic=<topic>&path=<path>
func removeSlotshandler(w http.ResponseWriter, req *http.Request) { return }

// POST /nodes/:id/services
func addServicesHandler(w http.ResponseWriter, req *http.Request) { return }

// POST /nodes/:id/slots
func addSlotsHandler(w http.ResponseWriter, req *http.Request) { return }

// This function will return a `Response` struct object to the node
func createNodeHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method == "PUT" {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			fmt.Fprintf(w, "Error : %s!", logError(err))
		}
		var nodeReq = new(proxy.NodeReq)
		err = json.Unmarshal(body, nodeReq)
		if err != nil {
			fmt.Fprintf(w, "Error : %s!", logError(err))
		}
		node, err := proxy.CreateNode(nodeReq)
		if err != nil {
			fmt.Fprintf(w, "Error : %s!", logError(err))
		}
		node.AddServices(nodeReq.Services)
		node.AddSlots(nodeReq.Slots)
		response := node.FormatResponse()
		js, err := json.Marshal(response)
		if err != nil {
			fmt.Fprintf(w, "Error : %s!", logError(err))
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
