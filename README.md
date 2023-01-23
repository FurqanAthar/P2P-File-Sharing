# Peer-to-Peer File Sharing System

This is a peer-peer file sharing system that is built using Distributed Hash Table (DHT) and consistent hashing. The system is designed to be a key-value storage system that can continue to operate in the face of failures, such as server crashes and broken networks.

## Key Features

**Distributed Hash Table:** The system uses DHT as a data structure for storing files in a distributed environment.

**Consistent Hashing:** The DHT is implemented using consistent hashing, which is a scheme for distributing key-value pairs, such that distribution does not depend upon the number of nodes in the DHT. This allows for easy scaling of the DHT.

**Failure Tolerance:** The system is designed to be failure-tolerant, meaning that it should not lose any data in case of a node failure.

**Basic Operations:** The system has only two basic operations; put() and get(). The put() operation takes as input a filename, evaluates its key using some hash function, and places the file on one of the nodes in the distributed system. The get() function takes as input a filename, finds the appropriate node that can potentially have that file and returns the file if it exists on that node.

## `DHT.py` Explanation:

1. `Node()`: Initialization of Node, you may maintain any state you need here. Following variables which are already declared should not be renamed and should be set appropriately since testing will take place through them. Some of them have already been set to appropriate values.
   a. `host:` Save hostname of Node e.g. localhost
   b. `port:` Save port of Node e.g. 8000
   c. `M`: hash value’s number of bits
   d. `N`: Size of the ring
   e. `key`: it is the hashed value of Node
   f. `successor`: Next node’s address (host, port)
   g. `predecessor`: Previous node’s address (host, port)
   h. `files`: Files mapped to this node
   i. `backUpFiles`: Files saved as back up on this node
2. `join()`: This function handles the logic for a new node joining the DHT: this function should update node’s successor and predecessor. It should also trigger the update of affected nodes’ (i.e. successor node and predecessor node) successor and predecessor too. Finally, the node should also get its share of files.

3. `leave()`: Calling leave should have the following effects: removal of node from the ring and transfer of files to the appropriate node.
4. `put()`: This function should handle placement of file on appropriate node; save the files in the directory for the node.
5. `get()`: This function is responsible for looking up and getting a file from the network.

Some functions have already been provided to you as utility functions. You can also create more helper functions.

1. `hasher()`: Hashes a string to an M bits long number. You should use it as follows:
   i. For a node: `hasher(node.host+str(node.port))`
   ii. For a file: `hasher(filename)`
2. `listener()`: This function listens for new incoming connections. For every new connection, it spins a new thread handleConnection to handle that connection. You may write any necessary logic for any connection there.
3. `sendFile()`: You can use this function to send a file over the socket to another node.
4. `receiveFile()`: This function can be used to receive files over the socket from other nodes. Both these functions have the following arguments:
   a. `soc`: A TCP socket object.
   b. `fileName`: File’s name along with complete path
