package main

import (
	"dcs/messages"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
)

/* how a storage node is represented on controller */
type node struct {
	state      bool
	last_time  time.Time
	space_aval int64
	host       string
	processed  int64
}

/* a chunk's info including the size of the chunk and the hosts it is stored on */
type chunk_info struct {
	size     int64
	hosts    []string
	position int64
}

// map of all active nodes (node_id -> node info)
var nodes = make(map[int64]node)

// map connecting file name, chunks, and where they are stored
var stash = make(map[string]map[string]chunk_info)

// map connecting file names to the size of all their content stored
var file_sizes = make(map[string]int64)

// map connection hosts to their node id
var node_host = make(map[string]int64)

// each storage node has to have a unique ID
var nodeID int64 = 0

// lock for nodes
var node_lock sync.Mutex

// lock for stash
var stash_lock sync.Mutex

// distinguishes connections from storage nodes vs the client (or a delete message)
func handleConnection(msgHandler *messages.MessageHandler) {
	defer msgHandler.Close()
	for {
		wrapper, _ := msgHandler.Receive()
		switch wrpr := interface{}(wrapper.Msg).(type) {
		case *messages.Wrapper_ClientController:
			handleClient(msgHandler, wrpr)
		case *messages.Wrapper_HeartBeat:
			handleNode(msgHandler, wrpr)
		case *messages.Wrapper_Delete:
			log.Printf("Deleting %s\n", wrpr.Delete.GetFilename())
			handleDelete(wrpr.Delete.GetFilename())
		case nil:
			return
		default:
			log.Printf("Unexpected message type: %T", wrapper)
			return
		}
	}
}

// handle messages from client
func handleClient(msgHandler *messages.MessageHandler, wrpr *messages.Wrapper_ClientController) {
	switch msg := interface{}(wrpr.ClientController.Msg).(type) {
	case *messages.C2C_StorageRequest:
		handleStorage(msgHandler, msg.StorageRequest.GetFileName(), msg.StorageRequest.GetSize(), msg.StorageRequest.GetChunk(), msg.StorageRequest.GetPosition())
	case *messages.C2C_RetrieveRequest:
		log.Println("Retrieval Request")
		handleRetrieve(msgHandler, msg.RetrieveRequest.GetFileName())
	case *messages.C2C_ListRequest:
		log.Println("LS Request")
		handleList(*msgHandler)
	case *messages.C2C_ListNodesRequest:
		log.Println("Node List Request")
		handleNodeList(msgHandler)
	default:
		log.Fatal("Bad Message Type while handling client")
		os.Exit(-1)
	}
}

// handle messages from storage nodes
func handleNode(msgHandler *messages.MessageHandler, wrpr *messages.Wrapper_HeartBeat) {
	switch msg := interface{}(wrpr.HeartBeat.Msg).(type) {
	case *messages.Beat_ConnectMessage:
		handleConnect(msgHandler, msg.ConnectMessage.GetSpace(), msg.ConnectMessage.GetHost())
	case *messages.Beat_ThumpMessage:
		handleThump(msgHandler, msg.ThumpMessage.GetNodeid(), msg.ThumpMessage.GetSpace(), msg.ThumpMessage.GetProcessed())
	case *messages.Beat_ChunkRequest:
		handleChunkRequest(msgHandler, msg.ChunkRequest.GetChunkId(), msg.ChunkRequest.GetHost())
	default:
		log.Fatal("Bad Message Type while handling heart beat")
		os.Exit(-1)
	}
}

// handle a connect request from a storage node (fill a new node with the aproprate data, add it to the node map, send response to node)
func handleConnect(msgHandler *messages.MessageHandler, space int64, host string) {
	log.Printf("Host: %s has been connected\n", host)
	var current_node node
	current_node.last_time = time.Now()
	current_node.state = true
	current_node.space_aval = space
	current_node.host = host
	node_lock.Lock()
	node_id := nodeID
	node_host[host] = node_id
	nodes[node_id] = current_node
	nodeID++
	node_lock.Unlock()
	sendOkay(msgHandler, true, node_id)
}

// handle a heart beat from a storage node
func handleThump(msgHandler *messages.MessageHandler, node_id int64, space int64, processed int64) {
	node_lock.Lock()
	defer node_lock.Unlock()
	old_node, exist := nodes[node_id]
	if !exist {
		sendOkay(msgHandler, false, node_id)
		return
	}
	if !old_node.state {
		sendOkay(msgHandler, false, node_id)
		return
	}
	if time.Now().Sub(old_node.last_time) > (10 * time.Second) {
		old_node.state = false
		nodes[node_id] = old_node
		log.Printf("(Disconnected: %d\n)", node_id)
		replicateHost(nodes[node_id].host)
		sendOkay(msgHandler, false, node_id)
		return
	} else {
		old_node.processed = processed
		old_node.last_time = time.Now()
		old_node.space_aval = space
		nodes[node_id] = old_node
		sendOkay(msgHandler, true, node_id)
		return
	}
}

// sends a response to a heart beat / connection
func sendOkay(msgHandler *messages.MessageHandler, okay bool, node_id int64) {
	msg := messages.Bump{Okay: okay, Nodeid: node_id}
	inner_wrapper := &messages.Beat{
		Msg: &messages.Beat_BumpMessage{BumpMessage: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_HeartBeat{HeartBeat: inner_wrapper},
	}
	msgHandler.Send(wrapper)
}

// handle a retrieval request from the client
func handleRetrieve(msgHandler *messages.MessageHandler, file_name string) {
	stash_lock.Lock()
	defer stash_lock.Unlock()
	chunks, exists := stash[file_name]
	if !exists {
		retrieveResponse(msgHandler, -1, make(map[string]chunk_info))
		return
	}
	retrieveResponse(msgHandler, file_sizes[file_name], chunks)
}

// handle a storage request from the client
func handleStorage(msgHandler *messages.MessageHandler, file_name string, size int64, input_chunk string, position int64) {
	//get random available storage node addresses
	main_host, rep_host_1, rep_host_2 := getAvalableHosts(size)

	//make sure that we actually got the hosts
	if main_host == "" || rep_host_1 == "" || rep_host_2 == "" {
		log.Println("Could not find enough hosts")
		storageResponse(msgHandler, false, "err", "err", "err")
		return
	}

	// put together all the needed chunk info
	var chunk chunk_info
	chunk.hosts = append(chunk.hosts, main_host, rep_host_1, rep_host_2)
	chunk.size = size
	chunk.position = position

	stash_lock.Lock()
	defer stash_lock.Unlock()
	//check if file-chunk is already stored (if it does tell client)
	chunk_map, file_exists := stash[file_name]
	if file_exists {
		_, chunk_exists := chunk_map[input_chunk]
		if chunk_exists {
			log.Println("The file: " + file_name + " already exists")
			storageResponse(msgHandler, false, "err", "err", "err")
			return
		}
		//the file exists but the current chunk does not
		chunk_map[input_chunk] = chunk
		file_sizes[file_name] += size
		storageResponse(msgHandler, true, main_host, rep_host_1, rep_host_2)
	} else {
		//the file doesn't exists yet, add it
		stash[file_name] = make(map[string]chunk_info)
		stash[file_name][input_chunk] = chunk
		file_sizes[file_name] = size
		storageResponse(msgHandler, true, main_host, rep_host_1, rep_host_2)
	}
}

// response to a retrieval request from the client (size -1 indicates retrieval failure)
func retrieveResponse(msgHandler *messages.MessageHandler, size int64, chunks map[string]chunk_info) {
	//convert our data type to the proto data type
	chunks_msg := make(map[string]*messages.ChunkInfo)
	for chunk_id, current_chunk_info := range chunks {
		chunk_info_msg := &messages.ChunkInfo{Size: current_chunk_info.size, Hosts: current_chunk_info.hosts, Position: current_chunk_info.position}
		chunks_msg[chunk_id] = chunk_info_msg
	}

	msg := messages.RetrieveResponse{Size: size, Chunks: chunks_msg}
	inner_wrapper := &messages.C2C{
		Msg: &messages.C2C_RetrieveResponse{RetrieveResponse: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ClientController{ClientController: inner_wrapper},
	}
	msgHandler.Send(wrapper)
}

// response to a storage request from client
func storageResponse(msgHandler *messages.MessageHandler, okay bool, main_host string, rep_host_1 string, rep_host_2 string) {
	msg := messages.StorageResponse{Okay: okay, Host: main_host, RepHost1: rep_host_1, RepHost2: rep_host_2}
	inner_wrapper := &messages.C2C{
		Msg: &messages.C2C_StorageResponse{StorageResponse: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ClientController{ClientController: inner_wrapper},
	}
	msgHandler.Send(wrapper)
}

// given a size of a chunk returns three hosts which are active and have space for the chunk
func getAvalableHosts(size int64) (string, string, string) {
	var main_host string
	var rep_host_1 string
	var rep_host_2 string
	host_count := 0

	node_lock.Lock()
	//put together a list of all available nodes
	var available_nodes []node
	for _, check_node := range nodes {
		if check_node.state && check_node.space_aval > size {
			available_nodes = append(available_nodes, check_node)
		}
	}
	node_lock.Unlock()

	for i := range available_nodes {
		j := rand.Intn(i + 1)
		available_nodes[i], available_nodes[j] = available_nodes[j], available_nodes[i]
	}

	//if we do not have enough storage nodes to replicate we reject the client
	if len(available_nodes) < 3 {
		log.Fatal("Not Enough Storage Nodes available\n")
		return "", "", ""
	}
	//pick three random nodes to store the chunk and its replicas on
	for aval_node := range available_nodes {
		if available_nodes[aval_node].state && available_nodes[aval_node].space_aval > size {
			if host_count == 0 {
				main_host = available_nodes[aval_node].host
			} else if host_count == 1 {
				rep_host_1 = available_nodes[aval_node].host
			} else if host_count == 2 {
				rep_host_2 = available_nodes[aval_node].host
			}

			var new_node node
			new_node.state = false
			available_nodes[aval_node] = new_node

			if host_count == 2 {
				return main_host, rep_host_1, rep_host_2
			}
			host_count++
		}
	}
	log.Fatal("Unexpected Storage Failure\n")
	return "", "", ""
}

// Given a chunk id and a host, sends response with a new host that same chunk is store on (for corruption purposes)
func handleChunkRequest(msgHandler *messages.MessageHandler, chunk_id string, original_host string) {
	for filename := range stash {
		for chunk := range stash[filename] {
			if chunk == chunk_id {
				for host := range stash[filename][chunk].hosts {
					if stash[filename][chunk].hosts[host] != original_host {
						msg := messages.ChunkResponse{Host: stash[filename][chunk].hosts[host]}
						inner_wrapper := messages.Beat{
							Msg: &messages.Beat_ChunkReponse{ChunkReponse: &msg},
						}
						wrapper := messages.Wrapper{
							Msg: &messages.Wrapper_HeartBeat{HeartBeat: &inner_wrapper},
						}
						msgHandler.Send(&wrapper)
						msgHandler.Close()
						return
					}
				}
			}
		}
	}
	log.Panic("Chunk not found")
}

// replicates all of the data from a dead node across nodes still active
func replicateHost(host string) {
	log.Printf("%s has died replicating its data now...", host)
	for filename := range stash {
		for chunk_id, curr_chunk_info := range stash[filename] {
			var node_with_chunk node
			var new_host string
			contains_host := false
			var old_host_index int
			//find if a chunk is stored on the dead node
			for curr_host := range curr_chunk_info.hosts {
				if host == curr_chunk_info.hosts[curr_host] {
					old_host_index = curr_host
					contains_host = true
					break
				}
			}
			//if the chunk is stored on a dead node replicate to a node it is not yet stored on
			if contains_host {
			host_search:
				for curr_node := range nodes {
					for curr_host := range curr_chunk_info.hosts {
						if curr_chunk_info.hosts[curr_host] != host {
							if nodes[curr_node].host == curr_chunk_info.hosts[curr_host] {
								if nodes[curr_node].state {
									node_with_chunk = nodes[curr_node]
								find_hosts:
									for {
										new_host, _, _ = getAvalableHosts(curr_chunk_info.size)
										for host_check := range curr_chunk_info.hosts {
											if curr_chunk_info.hosts[host_check] == new_host {
												goto find_hosts
											}
										}
										break
									}
									log.Printf("replacing %s with %s from %s for chunk: %s\n", curr_chunk_info.hosts[old_host_index], new_host, node_with_chunk.host, chunk_id)
									curr_chunk_info.hosts[old_host_index] = new_host
									msgHandler := connect(node_with_chunk.host)
									msg := messages.NodeCollapse{Chunk: chunk_id, NewHost: new_host}
									wrapper := &messages.Wrapper{
										Msg: &messages.Wrapper_NodeCollapse{NodeCollapse: &msg},
									}
									msgHandler.Send(wrapper)
									msgHandler.Close()
									break host_search
								}
							}
						}
					}
				}
			}
		}
	}
}

// tells client a list of all files stored
func handleList(msgHandler messages.MessageHandler) {
	var files []string
	for filename := range stash {
		files = append(files, filename)
	}
	msg := messages.ListFiles{Filename: files}
	inner_wrapper := &messages.C2C{
		Msg: &messages.C2C_ListFiles{ListFiles: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ClientController{ClientController: inner_wrapper},
	}
	msgHandler.Send(wrapper)
	msgHandler.Close()
}

// tells client all active nodes and their metadata
func handleNodeList(msgHandler *messages.MessageHandler) {
	var nodes_info []*messages.NodeInfo
	for node_id := range nodes {
		if nodes[node_id].state {
			node_msg := messages.NodeInfo{Host: nodes[node_id].host, Processed: nodes[node_id].processed, SpaceAval: nodes[node_id].space_aval}
			nodes_info = append(nodes_info, &node_msg)
		}
	}
	msg := messages.ListNodesResponse{Nodes: nodes_info}
	inner_wrapper := &messages.C2C{
		Msg: &messages.C2C_ListNodesReponse{ListNodesReponse: &msg},
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ClientController{ClientController: inner_wrapper},
	}
	msgHandler.Send(wrapper)
	msgHandler.Close()
}

// tells all nodes to delete the chunks of a file and then removes file from stash
func handleDelete(filename string) {
	for chunk := range stash[filename] {
		for host_index := range stash[filename][chunk].hosts {
			msgHandler := connect(stash[filename][chunk].hosts[host_index])
			msg := messages.Delete{Filename: chunk}
			wrapper := &messages.Wrapper{
				Msg: &messages.Wrapper_Delete{Delete: &msg},
			}
			msgHandler.Send(wrapper)
			msgHandler.Close()
		}
	}
	stash_lock.Lock()
	delete(stash, filename)
	stash_lock.Unlock()
}

// connects to a given host
func connect(host string) *messages.MessageHandler {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		log.Fatalln(err.Error())
	}

	return messages.NewMessageHandler(conn)
}

func main() {
	listener, err := net.Listen("tcp", ":13000")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	log.Println("Initiating Controller...")
	go checkNodes()
	for {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := messages.NewMessageHandler(conn)
			go handleConnection(msgHandler)
		}
	}
}

// repeatedly checks if nodes are still alive
func checkNodes() {
	for node_id := range nodes {
		if nodes[node_id].state {
			old_node := nodes[node_id]
			if time.Now().Sub(old_node.last_time) > (10 * time.Second) {
				old_node.state = false
				nodes[node_id] = old_node
				log.Printf("(Disconnected: %s)\n", nodes[node_id].host)
				replicateHost(nodes[node_id].host)
			}
		}
	}
	time.Sleep(5 * time.Second)
	checkNodes()
}
