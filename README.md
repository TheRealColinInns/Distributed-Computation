# Distributed-Computation
My personal implementation of HADOOP inspired distributed computation engine.

# Design and Usage

INIT:
 - Storage nodes must make initial connection to the controller, will be given an ID and added to the index of storage nodes and tell the controller its host (how to reach it)
 - Connection locations must be updated manually for each file
 - Storage nodes must send a heart beat every 5 seconds, after 10 seconds with no heart beat the controller will consider the node dead
 - Each heart beat must contain the space available on the node and the number of requests processed
 - If a node dies it must go through the connecting process again
 
Storage:
 - Client sends meta data for file they wish to store to controller as well as a chunk size (PUT request)
 - Controller responds with the locations that the client will send each chunk (Put response)
 - Client then sends these chunks to storage nodes (Store Request)
 - Storage nodes will store each chunk and replicate them on other storage nodes and respond with success/failure (Store response)
  
Retrieval:
 - Client sends meta data for file they wish to retrieve (GET request)
 - Controller responds with the locations that each chunk is stored (GET response)
 - Client requests the chunks from the storage nodes at the location specified by the controller (Retrieve request)

INTERFACE:
 - Store: ./client put \<file\> \<chunk size\>*
 - Retrieve: ./client get \<file\>
 - Delete: ./client delete \<file\>
 - List Files: ./client ls
 - List Nodes: ./client lsn
 
STRUCTURE:
 - Controller must be deployed somewhere visible so everything can find it
 - Controller must be running before storage nodes are started
 - Storage nodes have the ability it communicate with any other storage node
 - Storage nodes must be started from the root directory on orion
