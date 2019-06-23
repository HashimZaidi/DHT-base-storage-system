# DHT-base-storage-system

### How to run a node?

1. Go to the node's directory.
2. Run the command 'python node.py'
3. Either start a new network by pressing 1 or join an already existing network by pressing 2 and inputing a port number.

### How to leave a network?

1. Enter 'exit'.

### How to download a file in the network?

1. Enter 'download'.
2. Enter the name of the file.
3. If the file exists in the network it will be downloaded to the current node's directory

### How to add a new node?

1. Make a new folder and copy paste the node.py file into that folder.
2. Give the node a unique port number in line 326 in the Main function
3. Run the command 'hash(('127.0.0.1',port_number))%32' and rename the folder to the result. This would be the node id.

### Note: 
I've already included a node with id 5 as an example. You can add other nodes similarly