package main

import (
	"bytes"
	"crypto/md5"
	"dcs/messages"
	"encoding/hex"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// host to connect to controller
var controller_host string = "orion10:13000"
var port string = ":13000"

// remaining process count
var reducers_remaining int = 0
var mappers_remaining int = 0

var reducer_hosts []string

var write_lock sync.Mutex

// lock for counting remaining processes
var remaining_lock sync.Mutex
var output_lock sync.Mutex

func execute(filename string, jobname string, method string, num_reducers int) {
	log.Println("Executing...")
	reducers_remaining = num_reducers
	jobpath, err := filepath.Abs(jobname)
	if err != nil {
		panic(err)
	}
	hosts, reducers := getFileHosts(filename, num_reducers)
	reducer_hosts = reducers
	mappers_remaining = len(hosts)
	log.Printf("Reducers: %s\n", reducers)
	for host, chunks := range hosts {
		log.Println("A mapper just started...")
		msgHandler := connect(host)
		msg := messages.Execute{Chunks: chunks, JobName: jobpath, Method: method, Reducers: reducers}
		inner_wrapper := &messages.Beat{
			Msg: &messages.Beat_Execute{Execute: &msg},
		}
		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_HeartBeat{HeartBeat: inner_wrapper},
		}
		msgHandler.Send(wrapper)
		msgHandler.Close()
	}
	finish(method)
}

func getFileHosts(filename string, num_reducers int) (map[string][]string, []string) {
	msgHandler := connect(controller_host)
	msg := messages.RetrieveRequest{FileName: filename}
	inner_wrapper := &messages.C2C{
		Msg: &messages.C2C_RetrieveRequest{RetrieveRequest: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ClientController{ClientController: inner_wrapper},
	}
	msgHandler.Send(wrapper)
	resp_wrapper, _ := msgHandler.Receive()
	resp_msg := resp_wrapper.GetClientController().GetRetrieveResponse()
	msgHandler.Close()

	execute_hosts := make(map[string][]string)
	var reducers []string
	i := 0
	for chunk_id, chunk_info := range resp_msg.GetChunks() {
		//for each chunk id get a random host
		rnd_host := chunk_info.Hosts[i%len(chunk_info.Hosts)]
		execute_hosts[rnd_host] = append(execute_hosts[chunk_info.Hosts[i%len(chunk_info.Hosts)]], chunk_id)
		if len(reducers) < num_reducers {
			for potential_red := range chunk_info.Hosts {
				useable := true
				for curr_red := range reducers {
					if reducers[curr_red] == chunk_info.Hosts[potential_red] {
						useable = false
					}
				}
				if useable {
					reducers = append(reducers, chunk_info.Hosts[potential_red])
				}
				if len(reducers) == num_reducers {
					break
				}
			}
		}
		i++
	}
	return execute_hosts, reducers
}

// handle responses from storage nodes
func handleResponse(msgHandler *messages.MessageHandler) {
	for msgHandler != nil {
		wrapper, err := msgHandler.Receive()
		if err != nil {
			panic(err)
		}
		switch wrpr := interface{}(wrapper.Msg).(type) {
		case *messages.Wrapper_HeartBeat:
			switch w := interface{}(wrpr.HeartBeat.Msg).(type) {
			case *messages.Beat_Report:
				handleReport(msgHandler, int(w.Report.GetReportType()), w.Report.GetJobName(), w.Report.GetMethod())
			case *messages.Beat_Results:
				handleResults(msgHandler, w.Results.GetResults(), w.Results.GetJobName(), w.Results.GetMethod())
			default:
				log.Fatal("Unknown beat: %T\n", wrpr)
			}
		default:
			log.Fatalf("Unkown message: %T\n", wrapper.Msg)
		}
	}
}

// handle a report
func handleReport(msgHandler *messages.MessageHandler, report_type int, job_name string, method string) {
	if report_type == 1 {
		log.Println("A mapper just finished!")
		remaining_lock.Lock()
		mappers_remaining--
		remaining_lock.Unlock()
		if mappers_remaining == 0 {
			startReducers(job_name, method)
		}
	} else if report_type == 2 {
		log.Println("A reducer just started...")
	} else if report_type == 3 {
		log.Println("A reducer just finished!")
		remaining_lock.Lock()
		reducers_remaining--
		remaining_lock.Unlock()
	} else if report_type == 4 {
		log.Fatalf("Error with given job (%s) or method (%s)\n", job_name, method)
	} else {
		log.Fatal("Unrecognized report")
	}
}

// starts all the reducers
func startReducers(job_name string, method string) {
	log.Println("Starting Reducers...")
	for reducer := range reducer_hosts {
		msgHandler := connect(reducer_hosts[reducer])
		msg := messages.Reduce{JobName: job_name, Method: method}
		inner_wrapper := &messages.Beat{
			Msg: &messages.Beat_Reduce{Reduce: &msg},
		}
		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_HeartBeat{HeartBeat: inner_wrapper},
		}
		msgHandler.Send(wrapper)
		msgHandler.Close()
	}
}

// print the results of computation
func handleResults(msgHandler *messages.MessageHandler, results []byte, job_name string, method string) {
	file, err := os.OpenFile(method+"_results.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	write_lock.Lock()
	file.Write([]byte(results))
	write_lock.Unlock()
	file.Close()
}

// connects to a host
func connect(host string) *messages.MessageHandler {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		log.Println("Connection failure: " + err.Error())
	}
	return messages.NewMessageHandler(conn)
}

// waits until threads are finished
func finish(method string) {
	for {
		time.Sleep(5 * time.Second)
		if reducers_remaining == 0 {
			chunkFile(method+"_results.txt", 4096)
			log.Printf("Results are Ready! (check the DFS for %s_results.txt)\n", method)
			err := os.Remove(method + "_results.txt")
			if err != nil {
				panic(err)
			}
			os.Exit(1)
		}
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
			go handleResponse(msgHandler)
		}
	}
}

func main() {
	if len(os.Args) > 4 {
		red, err := strconv.Atoi(os.Args[4])
		if err != nil {
			panic(err)
		}
		go connectClient()
		execute(os.Args[1], os.Args[2], os.Args[3], red)
	} else {
		log.Fatalln("Needs a file, job file, and job method to function")
	}
}

// +--------- methods from client ----------+

// lock by node if the node has the max connections made
var node_handlers map[string]*messages.MessageHandler = make(map[string]*messages.MessageHandler)
var handler_lock sync.Mutex

// determines file type and converts it into chunks accordingly
func chunkFile(file_name string, max_chunk_size int64) {
	ext_type := filepath.Ext(file_name)
	if ext_type == ".txt" || ext_type == ".log" {
		chunkTextFile(file_name, max_chunk_size)
	} else {
		chunkRawFile(file_name, max_chunk_size)
	}
}

// breaks a text file into chunks (based of lines) and attempts to store them
func chunkTextFile(file_name string, max_chunk_size int64) {
	log.Printf("Chunking text file %s\n", file_name)
	file, err := os.Open(file_name)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	file_name = filepath.Base(file_name)
	msgHandler := connect(controller_host)
	prev_chunk := make([]byte, max_chunk_size)
	chunk_counter := 0
	line_counter := 1
	prev_chunk_size := 0

	for {
		chunk_buffer := make([]byte, max_chunk_size)
		bytes_read, err := file.Read(chunk_buffer)
		if bytes_read == 0 {
			file.Close()
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		chunked_lines, next_chunk_size, is_end := parseLines(chunk_buffer, prev_chunk, prev_chunk_size, bytes_read-1, max_chunk_size)
		if next_chunk_size < 0 { //this means the line was too large and we are skipping it
			continue
		}
		var chunk_size int64 = int64(prev_chunk_size + (bytes_read - next_chunk_size))
		storeChunk(file_name, chunk_size, chunked_lines, int64(chunk_counter), msgHandler)
		new_line := []byte{'\n'}
		line_counter += bytes.Count(chunked_lines, new_line)
		chunk_counter++
		prev_chunk_size = next_chunk_size
		if is_end {
			file.Close()
			break
		}
	}
	log.Printf("%s has been written\n", file_name)
	for _, handler := range node_handlers {
		handler.Close()
	}
	msgHandler.Close()
}

// raw data is parsed so individual file lines are not broken up
func parseLines(raw_data []byte, prev_chunk []byte, prev_chunk_size int, bytes_read int, max_chunk_size int64) ([]byte, int, bool) {

	//if the bytes read is less than the max chunk size we know the file is ended
	if bytes_read < int(max_chunk_size)-1 || raw_data[bytes_read] == 0 {
		chunked_lines := make([]byte, prev_chunk_size+bytes_read)
		chunked_lines = append(chunked_lines, prev_chunk...)
		chunked_lines = append(chunked_lines, raw_data...)
		return chunked_lines, 0, true
	}

	//if we are perfectly on a new line we don't need to change anything
	if raw_data[bytes_read] == '\n' {
		chunked_lines := make([]byte, prev_chunk_size+bytes_read)
		chunked_lines = append(chunked_lines, prev_chunk...)
		chunked_lines = append(chunked_lines, raw_data...)
		prev_chunk = nil
		return chunked_lines, 0, false
	}

	//otherwise we need to take out the last bytes not followed by a new line and store them for the next chunk
	for i := bytes_read; i >= 0; i-- {
		if raw_data[i] == '\n' {
			chunked_lines := make([]byte, prev_chunk_size+(bytes_read-i))
			chunked_lines = append(chunked_lines, prev_chunk...)
			chunked_lines = append(chunked_lines, raw_data[:i+1]...)
			copy(prev_chunk, make([]byte, max_chunk_size))
			copy(prev_chunk, raw_data[i+1:])
			return chunked_lines, bytes_read - i + 1, false
		}
	}
	log.Printf("Chunk size of %d is to small for your file, chunking file without line that is too large...\n", max_chunk_size)
	chunk_checksum := md5.Sum(raw_data)
	chunk_id := hex.EncodeToString(chunk_checksum[:])
	log.Printf("{%s}", chunk_id)
	return make([]byte, 0), -1, false
}

// breaks a file into chunks and attempts to store them
func chunkRawFile(file_name string, max_chunk_size int64) {
	log.Printf("Chunking raw file %s\n", file_name)
	file, err := os.Open(file_name)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	file_name = filepath.Base(file_name)

	fileInfo, _ := file.Stat()
	var fileSize int64 = fileInfo.Size()

	total_chunks := int64(math.Ceil(float64(fileSize) / float64(max_chunk_size)))
	msgHandler := connect(controller_host)

	for i := int64(0); i < total_chunks; i++ {
		//get either chunk size or what ever is left if it is less than the chunk size
		chunk_size := int64(math.Min(float64(max_chunk_size), float64(fileSize-int64(i*max_chunk_size))))
		chunk_buffer := make([]byte, chunk_size)
		file.Read(chunk_buffer)
		storeChunk(file_name, chunk_size, chunk_buffer, i, msgHandler)
	}
	msgHandler.Close()
}

// stores a chunk
func storeChunk(file_name string, size int64, chunk []byte, position int64, msgHandler *messages.MessageHandler) {
	chunk_checksum := md5.Sum(chunk)
	chunk_id := hex.EncodeToString(chunk_checksum[:])
	sendStorageRequest(msgHandler, file_name, size, chunk_id, position)
	handleStorageResponse(msgHandler, size, chunk, chunk_id)
}

// sends a storage request to the controller
func sendStorageRequest(msgHandler *messages.MessageHandler, file_name string, size int64, chunk_id string, position int64) {
	msg := messages.StorageRequest{FileName: file_name, Size: size, Chunk: chunk_id, Position: position}
	inner_wrapper := &messages.C2C{
		Msg: &messages.C2C_StorageRequest{StorageRequest: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ClientController{ClientController: inner_wrapper},
	}
	msgHandler.Send(wrapper)
}

// handles the response the controller gives to the storage request
func handleStorageResponse(msgHandler *messages.MessageHandler, size int64, chunk []byte, chunk_id string) {
	wrapper, _ := msgHandler.Receive()
	resp_msg := wrapper.GetClientController().GetStorageResponse()
	if resp_msg.GetOkay() {
		sendPutRequest(size, chunk_id, chunk, resp_msg.Host, resp_msg.RepHost1, resp_msg.RepHost2)
	} else {
		log.Println("File Already exists")
		os.Exit(0)
	}
}

// sends a put request to a storage node
func sendPutRequest(size int64, chunk_id string, chunk []byte, host string, rep_host1 string, rep_host2 string) {
	handler_lock.Lock()
	msgHandler, exists := node_handlers[host]
	if !exists {
		node_handlers[host] = connect(host)
		msgHandler = node_handlers[host]
	}
	handler_lock.Unlock()
	msg := messages.PutRequest{Size: size, Chunk: chunk_id, RepHost1: rep_host1, RepHost2: rep_host2, Data: chunk}
	inner_wrapper := &messages.C2S{
		Msg: &messages.C2S_PutRequest{PutRequest: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ClientStorage{ClientStorage: inner_wrapper},
	}
	msgHandler.Send(wrapper)
	resp_wrapper, _ := msgHandler.Receive()
	resp_msg := resp_wrapper.GetClientStorage().GetPutResponse()
	if !resp_msg.GetOkay() {
		log.Fatal("Unable to put chunk")
	}
}
