package main

import (
	"encoding/json"
	"fmt"
	"gilmour-proxy/proxy"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
)

func logWriterError(w http.ResponseWriter, err error) {
	errStr := err.Error()
	log.Println(errStr)
	js := formatResponse("error", errStr)
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(js.([]byte)); err != nil {
		log.Println(err.Error())
	}
	return
}

// Delete Node
func deleteNodeHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	id := vars["id"]
	node, err := getNode(id)
	if err != nil {
		logWriterError(w, err)
		return
	}
	if err := proxy.DeleteNode(node); err != nil {
		logWriterError(w, err)
		return
	}
	js := formatResponse("status", "ok")
	w.Header().Set("Content-Type", "application/json")
	if _, err = w.Write(js.([]byte)); err != nil {
		log.Println(err.Error())
	}
}

func getNode(id string) (node *proxy.Node, err error) {
	nm := proxy.GetNodeMap()
	node, err = nm.Get(proxy.NodeID(id))
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
	vars := mux.Vars(req)
	id := vars["id"]
	node, err := getNode(id)
	if err != nil {
		logWriterError(w, err)
		return
	}
	response, err := node.GetServices()
	if err != nil {
		logWriterError(w, err)
		return
	}
	js := formatResponse("services", response)
	w.Header().Set("Content-Type", "application/json")
	if _, err = w.Write(js.([]byte)); err != nil {
		log.Println(err.Error())
	}
	return
}

// GET /nodes/:id/slots
func getSlotsHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	id := vars["id"]
	node, err := getNode(id)
	if err != nil {
		logWriterError(w, err)
		return
	}
	response, err := node.GetSlots()
	if err != nil {
		logWriterError(w, err)
		return
	}
	js := formatResponse("slots", response)
	w.Header().Set("Content-Type", "application/json")
	if _, err = w.Write(js.([]byte)); err != nil {
		log.Println(err.Error())
	}
	return
}

// DELETE /nodes/:id/services?topic=<topic>&path=<path>
func removeServicesHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	id := vars["id"]
	node, err := getNode(id)
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
	vars := mux.Vars(req)
	id := vars["id"]
	node, err := getNode(id)
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
	vars := mux.Vars(req)
	id := vars["id"]
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
	}
	service := new(proxy.ServiceMap)
	err = json.Unmarshal(body, service)
	if err != nil {
		fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
	}
	node, err := getNode(id)
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
	vars := mux.Vars(req)
	id := vars["id"]
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
	}
	slot := new(proxy.Slot)
	err = json.Unmarshal(body, slot)
	if err != nil {
		fmt.Fprintf(w, "Error : %s!", proxy.LogError(err))
	}
	node, err := getNode(id)
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
	r := mux.NewRouter()
	r.HandleFunc("/nodes", createNodeHandler)
	r.HandleFunc("/nodes/{id}/services", getServicesHandler).Methods("GET")
	r.HandleFunc("/nodes/{id}/slots", getSlotsHandler).Methods("GET")
	r.HandleFunc("/nodes/{id}/services", addServicesHandler).Methods("POST")
	r.HandleFunc("/nodes/{id}/slots", addSlotsHandler).Methods("POST")
	r.HandleFunc("/nodes/{id}/services", removeServicesHandler).Methods("DELETE")
	r.HandleFunc("/nodes/{id}/slots", removeSlotsHandler).Methods("DELETE")
	r.HandleFunc("/nodes/{id}", deleteNodeHandler).Methods("DELETE")
	http.Handle("/", r)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Println(err.Error())
	}
}
