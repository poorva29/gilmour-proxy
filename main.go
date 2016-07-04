package main

import (
	"encoding/json"
	"fmt"
	"gilmour-proxy/proxy"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strings"
)

var rServiceMatch = regexp.MustCompile(`services`) // Contains "abc"
var rSlotMatch = regexp.MustCompile(`slots`)       // Contains "abc"

func logWriterError(w io.Writer, err error) {
	errStr := err.Error()
	log.Println(errStr)
	if _, err := w.Write([]byte(errStr)); err != nil {
		log.Println(err.Error())
	}
	return
}

func urlNotFound(w io.Writer) {
	if _, err := w.Write([]byte("Unknown Pattern")); err != nil {
		log.Println(err.Error())
	}
}

// Delete Node
func deleteNodeHandler(w http.ResponseWriter, req *http.Request) { return }

// All the handler functions for TCP server
func nodeOperationsHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method == "GET" {
		switch {
		case rServiceMatch.MatchString(req.URL.Path):
			getServicesHandler(w, req)
		case rSlotMatch.MatchString(req.URL.Path):
			getSlotsHandler(w, req)
		default:
			urlNotFound(w)
		}
	} else if req.Method == "POST" {
		switch {
		case rServiceMatch.MatchString(req.URL.Path):
			addServicesHandler(w, req)
		case rSlotMatch.MatchString(req.URL.Path):
			addSlotsHandler(w, req)
		default:
			urlNotFound(w)
		}
	} else if req.Method == "DELETE" {
		switch {
		case rServiceMatch.MatchString(req.URL.Path):
			removeServicesHandler(w, req)
		case rSlotMatch.MatchString(req.URL.Path):
			removeSlotsHandler(w, req)
		default:
			urlNotFound(w)
		}
	} else {
		http.NotFound(w, req)
	}
	return
}

func getNode(reqURLPath string, suffixStr string) (node *proxy.Node, err error) {
	nm := proxy.GetNodeMap()
	prefixLen := len("/nodes/")
	suffixLen := strings.LastIndex(reqURLPath, suffixStr)
	nodeID := reqURLPath[prefixLen:suffixLen]
	node, err = nm.Get(proxy.NodeID(nodeID))
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

func getHandler(w http.ResponseWriter, req *http.Request, reqType string) {
	reqURLPath := req.URL.Path
	node, err := getNode(reqURLPath, "/"+reqType)
	if err != nil {
		logWriterError(w, err)
		return
	}
	response, err := node.GetServices()
	if err != nil {
		logWriterError(w, err)
		return
	}
	js := formatResponse(reqType, response)
	w.Header().Set("Content-Type", "application/json")
	if _, err = w.Write(js.([]byte)); err != nil {
		log.Println(err.Error())
	}
	return
}

// GET /nodes/:id/services
func getServicesHandler(w http.ResponseWriter, req *http.Request) {
	getHandler(w, req, "services")
}

// GET /nodes/:id/slots
func getSlotsHandler(w http.ResponseWriter, req *http.Request) {
	getHandler(w, req, "slots")
}

// DELETE /nodes/:id/services?topic=<topic>&path=<path>
func removeServicesHandler(w http.ResponseWriter, req *http.Request) {
	reqURLPath := req.URL.Path
	node, err := getNode(reqURLPath, "/services")
	if err != nil {
		logWriterError(w, err)
		return
	}
	topic := proxy.GilmourTopic(req.URL.Query().Get("topic"))
	services, err := node.GetServices()
	if err != nil {
		logWriterError(w, err)
		return
	}
	service := services[topic]
	err = node.RemoveService(topic, service)
	status := setResponseStatus(err)
	js := formatResponse("status", status)
	w.Header().Set("Content-Type", "application/json")
	if _, err = w.Write(js.([]byte)); err != nil {
		log.Println(err.Error())
	}
	return
}

// DELETE /nodes/:id/slots?topic=<topic>&path=<path>
func removeSlotsHandler(w http.ResponseWriter, req *http.Request) {
	reqURLPath := req.URL.Path
	node, err := getNode(reqURLPath, "/slots")
	if err != nil {
		logWriterError(w, err)
		return
	}
	topic := req.URL.Query().Get("topic")
	path := req.URL.Query().Get("path")
	slot := proxy.Slot{
		Topic: topic,
		Path:  path,
	}
	err = node.RemoveSlot(slot)
	status := setResponseStatus(err)
	js := formatResponse("status", status)
	w.Header().Set("Content-Type", "application/json")
	if _, err = w.Write(js.([]byte)); err != nil {
		log.Println(err.Error())
	}
	return
}

// POST /nodes/:id/services
func addServicesHandler(w http.ResponseWriter, req *http.Request) {
	reqURLPath := req.URL.Path
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
	}
	service := new(proxy.ServiceMap)
	err = json.Unmarshal(body, service)
	if err != nil {
		fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
	}
	node, err := getNode(reqURLPath, "/services")
	if err != nil {
		logWriterError(w, err)
		return
	}
	for topic, value := range *service {
		err := node.AddService(topic, value)
		status := setResponseStatus(err)
		js := formatResponse("status", status)
		w.Header().Set("Content-Type", "application/json")
		if _, err = w.Write(js.([]byte)); err != nil {
			log.Println(err.Error())
		}
	}
	return
}

// POST /nodes/:id/slots
func addSlotsHandler(w http.ResponseWriter, req *http.Request) {
	reqURLPath := req.URL.Path
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
	}
	slot := new(proxy.Slot)
	err = json.Unmarshal(body, slot)
	if err != nil {
		fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
	}
	node, err := getNode(reqURLPath, "/slots")
	if err != nil {
		logWriterError(w, err)
		return
	}
	err = node.AddSlot(*slot)
	status := setResponseStatus(err)
	js := formatResponse("status", status)
	w.Header().Set("Content-Type", "application/json")
	if _, err = w.Write(js.([]byte)); err != nil {
		log.Println(err.Error())
	}
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
		engine, err := proxy.MakeGilmour("127.0.0.1:6379")
		if err != nil {
			fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
		}
		node, err := proxy.CreateNode(nodeReq, engine)
		if err != nil {
			fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
		}
		if err = node.Start(); err != nil {
			fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
		}
		go proxy.NodeWatchdog(node)
		response := node.FormatResponse()
		js, err := json.Marshal(response)
		if err != nil {
			fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
		}
		w.Header().Set("Content-Type", "application/json")
		if _, err = w.Write(js); err != nil {
			log.Println(err.Error())
		}
	} else {
		http.NotFound(w, req)
	}
	return
}

func main() {
	http.HandleFunc("/nodes", createNodeHandler)
	http.HandleFunc("/nodes/", nodeOperationsHandler)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Println(err.Error())
	}
}
