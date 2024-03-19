package main

import (
	"bufio"
	"crypto/md5"
	"dcs/job"
	"dcs/messages"
	"encoding/binary"
	"encoding/hex"
	"io/ioutil"
	"log"
	"net"
	"os"
	"plugin"
	"sort"
	"sync"
	"syscall"
	"time"
)

// how to connect to this node (for the client & other nodes)
var host string
var port string

// the node id as given by the controller
var node_id int64 = -1

// the number of total requests this storage node has processed
var requests_processed int64

// the host to connect to the controller
var controller_host string = "orion10:13000"
var comp_host string = "orion02:13000"
var comp_conn *messages.MessageHandler
var comp_conned bool = false
var comp_lock sync.Mutex

// keep message handler alive for each node
var node_handlers map[string]*messages.MessageHandler = make(map[string]*messages.MessageHandler)
var handler_lock sync.Mutex

// organizes and sorts incoming reducing requests
var shuffler map[string][][]byte = make(map[string][][]byte)
var shuffler_lock sync.Mutex

var temp_lock sync.Mutex

// storage location
var storage_dir string = "bigdata/students/cdinns/"

// connects a storage node to the controller, will return the node ID to use for future communication
func connect(msgHandler *messages.MessageHandler) {
	//wrap and send connection request
	msg := messages.Connect{Space: getSpace(storage_dir), Host: host}
	inner_wrapper := &messages.Beat{
		Msg: &messages.Beat_ConnectMessage{ConnectMessage: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_HeartBeat{HeartBeat: inner_wrapper},
	}
	msgHandler.Send(wrapper)

	//wait for response back from server
	wrapper, _ = msgHandler.Receive()
	resp_msg := *wrapper.GetHeartBeat().GetBumpMessage()

	if resp_msg.GetOkay() {
		node_id = resp_msg.GetNodeid()
	} else {
		log.Fatal("Unable to connect to server: connection rejected")
	}
}

// sends a heart beat to the server, the beat contains information about the node
func sendThump(msgHandler *messages.MessageHandler) {
	msg := messages.Thump{Nodeid: node_id, Space: getSpace(storage_dir), Processed: requests_processed}
	inner_wrapper := &messages.Beat{
		Msg: &messages.Beat_ThumpMessage{ThumpMessage: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_HeartBeat{HeartBeat: inner_wrapper},
	}
	msgHandler.Send(wrapper)
}

// handles the response to a hearth beat from the controller
func handleBump(msgHandler *messages.MessageHandler) {
	wrapper, _ := msgHandler.Receive()
	resp_msg := wrapper.GetHeartBeat().GetBumpMessage()
	if !resp_msg.Okay {
		connect(msgHandler)
	} else {
		node_id = resp_msg.GetNodeid()
	}
}

// returns the amount of space in a directory
func getSpace(directory string) int64 {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(directory, &fs)
	if err != nil {
		log.Fatal("Cannot access " + directory)
		panic(err)
	}
	return int64(fs.Bfree * uint64(fs.Bsize))
}

// handles incoming connections from client, controller, and other nodes
func handleConnection(msgHandler *messages.MessageHandler) {
	defer msgHandler.Close()
	for {
		wrapper, _ := msgHandler.Receive()
		switch wrpr := interface{}(wrapper.Msg).(type) {
		case *messages.Wrapper_ClientStorage:
			handleClient(msgHandler, wrpr)
		case *messages.Wrapper_Replicate:
			handleNode(wrpr)
		case *messages.Wrapper_NodeCollapse:
			requests_processed++
			handleCollapse(wrpr.NodeCollapse.GetChunk(), wrpr.NodeCollapse.GetNewHost())
		case *messages.Wrapper_ReplicateRequest:
			requests_processed++
			handleReplicateRequest(msgHandler, wrpr.ReplicateRequest.GetChunkId())
		case *messages.Wrapper_Delete:
			requests_processed++
			err := os.Remove(storage_dir + wrpr.Delete.GetFilename())
			if err != nil {
				panic(err)
			}
		case *messages.Wrapper_HeartBeat:
			requests_processed++
			switch w := interface{}(wrpr.HeartBeat.Msg).(type) {
			case *messages.Beat_Reduce:
				reduce(w.Reduce.GetJobName(), w.Reduce.GetMethod())
			case *messages.Beat_Shuffle:
				shuffle(w.Shuffle.GetKey(), w.Shuffle.GetValue(), w.Shuffle.GetJobName(), w.Shuffle.GetMethod())
			case *messages.Beat_Execute:
				handleExecute(wrpr.HeartBeat.GetExecute().GetChunks(), wrpr.HeartBeat.GetExecute().GetJobName(), wrpr.HeartBeat.GetExecute().GetMethod(), wrpr.HeartBeat.GetExecute().GetReducers())
			}
		case nil:
			return
		default:
			log.Printf("Unexpected message type: %T", wrapper)
			return
		}
	}
}

// gets the data for a specific chunk and sends it back to the node that requested it
func handleReplicateRequest(msgHandler *messages.MessageHandler, chunk_id string) {
	file, err := os.Open(storage_dir + chunk_id)
	if err != nil {
		panic(err)
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}
	msg := messages.Replicate{ChunkId: chunk_id, Data: data}
	wrapper := &messages.Wrapper{Msg: &messages.Wrapper_Replicate{Replicate: &msg}}
	msgHandler.Send(wrapper)
}

// handles messages coming from the client
func handleClient(msgHandler *messages.MessageHandler, wrapper *messages.Wrapper_ClientStorage) {
	switch msg := interface{}(wrapper.ClientStorage.Msg).(type) {
	case *messages.C2S_PutRequest:
		requests_processed++
		handlePutRequest(msgHandler, msg.PutRequest.GetSize(), msg.PutRequest.GetChunk(), msg.PutRequest.GetData(), msg.PutRequest.GetRepHost1(), msg.PutRequest.GetRepHost2())
	case *messages.C2S_GetRequest:
		requests_processed++
		handleGetRequest(msgHandler, msg.GetRequest.GetChunk())
	default:
		log.Fatal("Bad Message Type while handling client")
		os.Exit(-1)
	}
}

// handles an incoming put request from the client
func handlePutRequest(msgHandler *messages.MessageHandler, size int64, chunk_id string, chunk []byte, rep_host1 string, rep_host2 string) {
	storeChunk(chunk_id, chunk)
	sendOkay(msgHandler, true)
	replicateToHost(rep_host1, chunk_id, chunk)
	replicateToHost(rep_host2, chunk_id, chunk)
}

// sends data for a chunk to a specific host
func replicateToHost(host string, chunk_id string, chunk []byte) {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	msgHandler := messages.NewMessageHandler(conn)

	msg := messages.Replicate{ChunkId: chunk_id, Data: chunk}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_Replicate{Replicate: &msg},
	}
	msgHandler.Send(wrapper)
	msgHandler.Close()
}

// tell client if put/get was successful
func sendOkay(msgHandler *messages.MessageHandler, okay bool) {
	msg := messages.PutResponse{Okay: okay}
	inner_wrapper := &messages.C2S{
		Msg: &messages.C2S_PutResponse{PutResponse: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ClientStorage{ClientStorage: inner_wrapper},
	}
	msgHandler.Send(wrapper)
}

// handles an incoming get request from the client
func handleGetRequest(msgHandler *messages.MessageHandler, chunk_id string) {
	file, err := os.Open(storage_dir + chunk_id)
	if err != nil {
		panic(err)
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}
	chunk_checksum := md5.Sum(data)
	chunk_id_checksum := hex.EncodeToString(chunk_checksum[:])
	if chunk_id != chunk_id_checksum {
		data = handleCorruption(chunk_id)
		log.Println("Corruption handled")
	}
	msg := messages.GetResponse{Data: data}
	inner_wrapper := &messages.C2S{
		Msg: &messages.C2S_GetResponse{GetResponse: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ClientStorage{ClientStorage: inner_wrapper},
	}
	msgHandler.Send(wrapper)
}

// replaces data if a corruption is found
func handleCorruption(chunk_id string) []byte {
	log.Printf("Corruption detected for chunk: %s\n", chunk_id)
	msgHandler, _ := connectNode(controller_host, true)
	msg := messages.ChunkRequest{ChunkId: chunk_id, Host: host}
	inner_wrapper := &messages.Beat{
		Msg: &messages.Beat_ChunkRequest{ChunkRequest: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_HeartBeat{HeartBeat: inner_wrapper},
	}
	msgHandler.Send(wrapper)
	resp, _ := msgHandler.Receive()
	new_msgHandler, _ := connectNode(resp.GetHeartBeat().GetChunkReponse().GetHost(), true)
	new_msg := &messages.ReplicateRequest{ChunkId: chunk_id}
	new_inner_wrapper := &messages.Wrapper_ReplicateRequest{ReplicateRequest: new_msg}
	new_wrapper := &messages.Wrapper{Msg: new_inner_wrapper}
	new_msgHandler.Send(new_wrapper)
	new_resp, _ := new_msgHandler.Receive()
	data := new_resp.GetReplicate().GetData()
	new_msgHandler.Close()
	msgHandler.Close()
	storeChunk(chunk_id, data)
	return data
}

// handles a connection from another node
func handleNode(wrapper *messages.Wrapper_Replicate) {
	storeChunk(wrapper.Replicate.GetChunkId(), []byte(wrapper.Replicate.GetData()))
}

// stores a chunk to the disk
func storeChunk(chunk_id string, chunk []byte) {
	file, err := os.Create(storage_dir + chunk_id)
	if err != nil {
		panic(err)
	}

	file.Write(chunk)
	file.Close()
}

// handles a node dying
func handleCollapse(chunk_id string, host string) {
	log.Printf("Replicating chunk: %s to %s\n", chunk_id, host)
	file, err := os.Open(storage_dir + chunk_id)
	if err != nil {
		panic(err)
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}

	replicateToHost(host, chunk_id, data)
}

// handles the mapping phase
func handleExecute(chunks []string, jobname string, method string, reducers []string) {
	log.Printf("Handling Execuation of %s\n", jobname)
	my_job, err := plugin.Open(jobname)
	if err != nil {
		log.Printf("The job file (%s) doesn't exists\n", jobname)
		sendReport(4, jobname, method)
		cleanup()
		return
	}
	symbol, err := my_job.Lookup(method)
	if err != nil {
		log.Printf("The method (%s) doesn't exists\n", method)
		sendReport(4, jobname, method)
		cleanup()
		return
	}
	get_exec, ok := symbol.(func() job.Executer)
	if !ok {
		panic("Wrong Symbol")
	}
	exec := get_exec()
	for i := 0; i < len(chunks); i++ {
		file, err := os.Open(storage_dir + chunks[i])
		if err != nil {
			panic(err)
		}
		line_num := 0
		scanner := bufio.NewScanner(file)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024)
		for scanner.Scan() {
			key, value := exec.Execute(line_num, scanner.Text())
			sendToRed(key, value, jobname, method, reducers)
			line_num++
		}
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
		file.Close()
	}
	sendReport(1, jobname, method)
}

// sends key value pair to reducer
func sendToRed(key []byte, value []byte, job_name string, method string, reducers []string) {
	padded_key := key
	if len(key) < 8 {
		padded_key = append(key, make([]byte, 8-len(key))...)
	}
	key_int := binary.LittleEndian.Uint64(padded_key)
	reducer_host := reducers[key_int%uint64(len(reducers))]
	handler_lock.Lock()
	msgHandler, exists := node_handlers[reducer_host]
	if !exists {
		node_handlers[reducer_host], _ = connectNode(reducer_host, true)
		msgHandler = node_handlers[reducer_host]
	}
	handler_lock.Unlock()
	msg := messages.Shuffle{Key: key, Value: value, JobName: job_name, Method: method}
	inner_wrapper := &messages.Beat{
		Msg: &messages.Beat_Shuffle{Shuffle: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_HeartBeat{HeartBeat: inner_wrapper},
	}
	msgHandler.Send(wrapper)
}

// shuffles around the mapping to reducers
func shuffle(key []byte, value []byte, job_name string, method string) {
	shuffler_lock.Lock()
	shuffler[string(key)] = append(shuffler[string(key)], value)
	shuffler_lock.Unlock()
}

// reduces the mappings given by shuffle
func reduce(job_name string, method string) {
	sendReport(2, job_name, method)

	my_job, err := plugin.Open(job_name)
	if err != nil {
		log.Printf("The job file (%s) doesn't exists\n", job_name)
		sendReport(4, job_name, method)
		cleanup()
		return
	}
	symbol, err := my_job.Lookup(method)
	if err != nil {
		log.Printf("The method (%s) doesn't exists\n", method)
		sendReport(4, job_name, method)
		cleanup()
		return
	}
	get_exec, ok := symbol.(func() job.Executer)
	if !ok {
		panic("Wrong Symbol")
	}
	exec := get_exec()

	keys := make([]string, 0, len(shuffler))
	for k := range shuffler {
		keys = append(keys, k)
	}
	sort.SliceStable(keys, func(i int, j int) bool { return len(shuffler[keys[i]]) > len(shuffler[keys[j]]) })

	for key := range keys {
		result_key, results := exec.Reduce([]byte(keys[key]), shuffler[keys[key]])
		result_string := exec.Results(result_key, results)
		temp_lock.Lock()
		file, err := os.OpenFile(storage_dir+method+"_tmp.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		file.Write([]byte(result_string))
		file.Close()
		temp_lock.Unlock()
	}
	returnResults(job_name, method)
	sendReport(3, job_name, method)
}

// sends results to the computational manager
func returnResults(job_name string, method string) {
	if !comp_conned {
		log.Println("Connecting to comp")
		comp_conn, _ = connectNode(comp_host, true)
		comp_conned = true
	}
	msgHandler := comp_conn
	results, err := os.ReadFile(storage_dir + method + "_tmp.txt")
	if err != nil {
		panic(err)
	}
	msg := messages.Results{Results: results, JobName: job_name, Method: method}
	inner_wrapper := &messages.Beat{
		Msg: &messages.Beat_Results{Results: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_HeartBeat{HeartBeat: inner_wrapper},
	}
	msgHandler.Send(wrapper)
	os.Remove(storage_dir + method + "_tmp.txt")
}

// sends a report to the computational manager
func sendReport(report_type int, job_name string, method string) {
	if !comp_conned {
		log.Println("Connecting to comp")
		var connected bool
		comp_conn, connected = connectNode(comp_host, false)
		if !connected {
			return
		}
		comp_conned = true
	}
	msgHandler := comp_conn
	msg := messages.Report{ReportType: int64(report_type), JobName: job_name, Method: method}
	inner_wrapper := &messages.Beat{
		Msg: &messages.Beat_Report{Report: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_HeartBeat{HeartBeat: inner_wrapper},
	}
	comp_lock.Lock()
	msgHandler.Send(wrapper)
	comp_lock.Unlock()
}

// connects to a given host
func connectNode(host string, crash bool) (*messages.MessageHandler, bool) {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		if crash {
			log.Fatalln(err.Error())
		} else {
			return nil, false
		}
	}
	return messages.NewMessageHandler(conn), true
}

// reset node
func cleanup() {
	node_handlers = make(map[string]*messages.MessageHandler)
	shuffler = make(map[string][][]byte)
	comp_conned = false
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalln("Invalid arguments")
		os.Exit(0)
	}
	port = os.Args[1]
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	host = hostname + port
	log.Println("Starting storage node")
	go controllerConnection(controller_host)
	connectClient()
}

// connect to the controller
func controllerConnection(controller_host string) {
	for {
		conn, err := net.Dial("tcp", controller_host)
		if err != nil {
			log.Fatalln(err.Error())
			return
		}
		msgHandler := messages.NewMessageHandler(conn)

		//send heart beat (will tell us if we need to connect)
		sendThump(msgHandler)
		handleBump(msgHandler)
		msgHandler.Close()
		time.Sleep(5 * time.Second)
	}
}

// connect to the client
func connectClient() {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	for {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := messages.NewMessageHandler(conn)
			go handleConnection(msgHandler)
		}
	}
}
