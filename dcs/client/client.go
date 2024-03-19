package main

import (
	"bytes"
	"crypto/md5"
	"dcs/messages"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// lock for chunks
var chunk_lock sync.Mutex

// keep message handler alive for each node
var node_handlers map[string]*messages.MessageHandler = make(map[string]*messages.MessageHandler)
var handler_lock sync.Mutex

// host to connect to controller
var controller_host string = "orion10:13000"

/* a chunk's info including the size of the chunk and the hosts it is stored on */
type chunk_info struct {
	size     int64
	hosts    []string
	position int64
}

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

// retrieves a the chunks of a file and recontructs the file
func retrieveFile(filename string) {
	msgHandler := connect(controller_host)
	msg := messages.RetrieveRequest{FileName: filename}
	inner_wrapper := &messages.C2C{
		Msg: &messages.C2C_RetrieveRequest{RetrieveRequest: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ClientController{ClientController: inner_wrapper},
	}
	msgHandler.Send(wrapper)

	var chunks = make(map[int64][]byte)
	resp_wrapper, _ := msgHandler.Receive()
	resp_msg := resp_wrapper.GetClientController().GetRetrieveResponse()
	msgHandler.Close()
	if len(resp_msg.GetChunks()) == 0 {
		log.Fatalf("%s does not exists in the DFS\n", filename)
	}
	for chunk_id, curr_chunk_info := range resp_msg.GetChunks() {
		constructFileArray(chunks, curr_chunk_info.GetPosition(), chunk_id, curr_chunk_info.GetHosts()[0])
	}
	buildFile(chunks, filename)
}

// synchronously contrusts the array that will build the file
func constructFileArray(chunks map[int64][]byte, position int64, chunk_id string, host string) {
	data := retrieveChunk(chunk_id, host)
	chunk_lock.Lock()
	chunks[position] = data
	chunk_lock.Unlock()
}

// retrieves a specific chunk from a storage node
func retrieveChunk(chunk_id string, host string) []byte {
	msgHandler := connect(host)
	msg := messages.GetRequest{Chunk: chunk_id}
	inner_wrapper := &messages.C2S{
		Msg: &messages.C2S_GetRequest{GetRequest: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ClientStorage{ClientStorage: inner_wrapper},
	}
	msgHandler.Send(wrapper)

	resp_wrapper, _ := msgHandler.Receive()
	resp_msg := resp_wrapper.GetClientStorage().GetGetResponse()
	msgHandler.Close()
	return resp_msg.GetData()
}

// contructs the file from the byte map
func buildFile(chunks map[int64][]byte, filename string) {
	file, err := os.Create("/home/cdinns/Documents/677/output/output_" + filename)
	if err != nil {
		panic(err)
	}
	for i := int64(0); i < int64(len(chunks)); i++ {
		file.Write(chunks[i])
	}
	log.Printf("File written to %s\n", "output_"+filename)
}

// connects to a host
func connect(host string) *messages.MessageHandler {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		log.Println("Connection failure: " + err.Error())
	}
	return messages.NewMessageHandler(conn)
}

// lists the files currently stored
func listFiles() {
	msgHandler := connect(controller_host)
	inner_wrapper := &messages.C2C{
		Msg: &messages.C2C_ListRequest{},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ClientController{ClientController: inner_wrapper},
	}
	msgHandler.Send(wrapper)
	resp, _ := msgHandler.Receive()
	file_list := resp.GetClientController().GetListFiles().GetFilename()
	for filename := range file_list {
		fmt.Printf("%s\n", file_list[filename])
	}
	msgHandler.Close()
}

// deep delete a file
func deleteFile(filename string) {
	msgHandler := connect(controller_host)
	msg := messages.Delete{Filename: filename}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_Delete{Delete: &msg},
	}
	msgHandler.Send(wrapper)
	msgHandler.Close()
}

// lists all nodes currently active and their metadata
func listNodes() {
	msgHandler := connect(controller_host)
	msg := messages.ListNodesRequest{}
	inner_wrapper := &messages.C2C{
		Msg: &messages.C2C_ListNodesRequest{ListNodesRequest: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ClientController{ClientController: inner_wrapper},
	}
	msgHandler.Send(wrapper)
	resp, _ := msgHandler.Receive()
	nodes := resp.GetClientController().GetListNodesReponse().GetNodes()
	for node := range nodes {
		fmt.Printf("Node Host: %s,\tRequests Processed: %d,\tSpace Available: %d\n", nodes[node].GetHost(), nodes[node].GetProcessed(), nodes[node].GetSpaceAval())
	}
	msgHandler.Close()
}

func main() {
	if len(os.Args) > 2 {
		if os.Args[1] == "put" {
			if len(os.Args) > 3 {
				chunk_size, err := strconv.ParseInt(os.Args[3], 10, 64)
				if err != nil {
					log.Panic("Cannot parse chunk size")
				}
				chunkFile(os.Args[2], chunk_size)
			} else {
				chunkFile(os.Args[2], 4096*8)
			}
		} else if os.Args[1] == "get" {
			retrieveFile(os.Args[2])
		} else if os.Args[1] == "del" {
			deleteFile(os.Args[2])
		}
	} else if len(os.Args) > 1 {
		if os.Args[1] == "ls" {
			listFiles()
		} else if os.Args[1] == "lsn" {
			listNodes()
		}
	}
}
