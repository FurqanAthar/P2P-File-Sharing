# While leaving:
# I backedup all my files at my successor node. I asked my successor to backup my files into its own self.files and replicate it.

# While node failure:
# When my node fails and the failed node's predecessor gets to know about that, it asked the failed node's successor to backedup failed node's
# files.

# REASON FOR FILES BACKUP AT SUCCESSOR:
# We have to backup our files to our successor node because we are considering the DHT ring moving in a clockwise direction and in this ring we
# always append the files closest to node in clockwise direction. So whenever a node fails its files will be backed up at its successor node
# because its successor is always closest in a ring in clockwise direction 

import socket 
import threading
import os
import time
import hashlib
from json import dumps, loads
from queue import Queue


class Node:
	def __init__(self, host, port):
		self.stop = False
		self.host = host
		self.port = port
		self.M = 16
		self.N = 2**self.M
		self.key = self.hasher(host+str(port))
		# You will need to kill this thread when leaving, to do so just set self.stop = True
		threading.Thread(target = self.listener).start()
		self.files = []
		self.backUpFiles = []
		if not os.path.exists(host+"_"+str(port)):
			os.mkdir(host+"_"+str(port))
		'''
		------------------------------------------------------------------------------------
		DO NOT EDIT ANYTHING ABOVE THIS LINE
		'''
		# Set value of the following variables appropriately to pass Intialization test
		self.successor = (self.host, self.port)
		self.predecessor = (self.host, self.port)

		# additional state variables
		self.address = (self.host, self.port)
		self.second_successor = (self.host, self.port)
		self.lookup_queue = Queue(0)
		self.join_queue = Queue(0)
		self.predecessor_queue = Queue(0)
		self.pinging_queue = Queue(1)

	def hasher(self, key):
		return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N


	def handleConnection(self, client, addr):
		data = client.recv(1024).decode("utf-8")
		data = loads(data)
		#print("DATA:", data)
		message_type = data[0]
		if message_type == "I am your predecessor":
			if data[1] == 0:
				soc = socket.socket()
				soc.connect((data[2][0], data[2][1]))
				new_message = ["My predecessor is", self.predecessor]
				new_message = dumps(new_message)
				soc.send(new_message.encode("utf-8"))
				self.predecessor = (data[2][0], data[2][1])
				soc.close()
			else:
				self.predecessor = (data[2][0], data[2][1])
				client.send("ok".encode("utf-8"))

		elif message_type == "My predecessor is":
			self.predecessor_queue.put(data[1])

		elif message_type == "I am your successor":
			self.successor = (data[2][0], data[2][1])

		elif message_type == "join":
			if self.address == self.successor and self.address == self.predecessor:
				# address coming as a list in list not as a tuple so accessing it using indices
				self.successor = (data[2][0], data[2][1])
				self.predecessor = (data[2][0], data[2][1])
				soc = socket.socket()
				soc.connect((data[2][0], data[2][1]))
				message = ["lookupdone", 1, self.address]
				soc.send(dumps(message).encode("utf-8"))
				soc.close()
			else:
				new_message = ["lookup", data[1], data[2]]
				self.look_up(new_message, new_message[1])

		elif message_type == "lookup":
			self.look_up(data, data[1])

		elif message_type == "lookupdone":
			self.join_queue.put(data)
		
		elif message_type == "Sending you my file":
			filename_recv = data[1]
			file_path = self.host + "_" + str(self.port) + "/" + filename_recv
			self.recieveFile(client, file_path)
			self.files.append(filename_recv)
			soc = socket.socket()
			soc.connect(self.successor)
			new_message = ["backupfile", [filename_recv]]
			new_message = dumps(new_message)
			soc.send(new_message.encode("utf-8"))
			time.sleep(1)
			self.sendFile(soc, file_path)
			soc.close()

		elif message_type == "get_file":
			file_name_recv = data[1]
			if file_name_recv not in self.files:
				client.send("None".encode("utf-8"))
			else:
				client.send("get_request_processing".encode("utf-8"))
				time.sleep(1)
				file_path = self.host + "_" + str(self.port) + "/" + file_name_recv
				self.sendFile(client,file_path)

		elif message_type == "My files share":
			files_list = [] # for new node
			key_hash = data[1]
			new_nodes_pred= (data[2][0], data[2][1])
			new_nodes_pred_key = self.hasher(new_nodes_pred[0] + str(new_nodes_pred[1]))
			for i in self.files:
				if self.hasher(i) <= key_hash and self.hasher(i) > new_nodes_pred_key:
					files_list.append(i)
					self.backUpFiles.append(i)
				if new_nodes_pred_key > key_hash:
					if self.hasher(i) <= key_hash or self.hasher(i) > new_nodes_pred_key:
						files_list.append(i)
						self.backUpFiles.append(i)
			
			for i in files_list:
				client.send("continue".encode("utf-8"))
				time.sleep(1)
				client.send(i.encode("utf-8"))
				file_path = self.host + "_" + str(self.port) + "/" + i
				time.sleep(1)
				self.sendFile(client, file_path)
				#os.remove(file_path)
				self.files.remove(i)
				time.sleep(1)
			client.send("stop".encode("utf-8"))
		
		elif message_type == "I am leaving":
			if data[1] == "My files":
				self.backUpFiles.clear()
				files_name_received = data[2]
				# receiving files from leaving node
				for i in files_name_received:
					file_path = self.host + "_" + str(self.port) + "/" + i
					self.recieveFile(client, file_path)
					self.files.append(i)
				
				# sending files update to its own successor
				soc = socket.socket()
				soc.connect(self.successor)
				new_message = dumps(["backupfile",files_name_received])
				soc.send(new_message.encode("utf-8"))
				for i in files_name_received:
					time.sleep(1)
					file_path = self.host + "_" + str(self.port) + "/" + i
					self.sendFile(soc, file_path)
					time.sleep(1)
				soc.close()
			else:
				self.successor = (data[1][0], data[1][1])
				soc = socket.socket()
				soc.connect(self.successor)
				new_message = ["I am your predecessor", 1, self.address]
				new_message = dumps(new_message)
				soc.send(new_message.encode("utf-8"))
				soc.recv(1024).decode("utf-8")
				soc.close()
			self.second_succ_helper()

		elif message_type == "Second successor":
			client.send(dumps(self.successor).encode("utf-8"))

		elif message_type == "backupfile":
			file_name = data[1]
			for i in file_name:
				file_path = self.host + "_" + str(self.port) + "/" + i
				self.recieveFile(client, file_path)
				self.backUpFiles.append(i)

		elif message_type == "Node fails, backup files now":
			for i in self.backUpFiles:
				self.files.append(i)

			# Backed up Files that are save into self.files are now send to successor to save as backup
			soc = socket.socket()
			soc.connect(self.successor)
			message = ["backupfile", self.backUpFiles]
			message = dumps(message)
			soc.send(message.encode("utf-8"))
			for i in self.backUpFiles:
				file_path = self.host + "_" + str(self.port) + "/" + i
				time.sleep(1)
				self.sendFile(soc, file_path)
			soc.close()
			self.backUpFiles.clear()
			self.second_succ_helper()

		elif message_type == "Pinging":
			client.send("ok".encode("utf-8"))

		elif message_type == "pinging reply":
			self.pinging_queue.put("ok")

	def listener(self):
		listener = socket.socket()
		listener.bind((self.host, self.port))
		listener.listen(10)
		while not self.stop:
			client, addr = listener.accept()
			threading.Thread(target = self.handleConnection, args = (client, addr)).start()
		print ("Shutting down node:", self.host, self.port)
		try:
			listener.shutdown(2)
			listener.close()
		except:
			listener.close()

	
	def look_up(self, message, key_needed):	
		successor_key = self.hasher(self.successor[0] + str(self.successor[1])) 
		if self.key < successor_key:
			if self.key < successor_key < key_needed:
				soc = socket.socket()
				soc.connect(self.successor)
				message = dumps(message)
				soc.sendto(message.encode("utf-8"), self.successor)
				soc.close()
			elif key_needed < successor_key and key_needed < self.key:
				soc = socket.socket()
				soc.connect(self.successor)
				message = dumps(message)
				soc.sendto(message.encode("utf-8"), self.successor)
				soc.close()
			else:
				soc = socket.socket()
				soc.connect((message[2][0],message[2][1]))
				new_message = ["lookupdone", 2, self.successor]
				new_message = dumps(new_message)
				soc.send(new_message.encode("utf-8"))
				soc.close()
		elif self.key > successor_key:
			if key_needed > self.key:
				soc = socket.socket()
				soc.connect((message[2][0],message[2][1]))
				new_message = ["lookupdone", 2, self.successor]
				new_message = dumps(new_message)
				soc.send(new_message.encode("utf-8"))
				soc.close()
			elif key_needed < successor_key:
				soc = socket.socket()
				soc.connect((message[2][0],message[2][1]))
				new_message = ["lookupdone", 2, self.successor]
				new_message = dumps(new_message)
				soc.send(new_message.encode("utf-8"))
				soc.close()
			else:
				soc = socket.socket()
				soc.connect(self.successor)
				message = dumps(message)
				soc.sendto(message.encode("utf-8"), self.successor)
				soc.close()


	def join(self, joiningAddr):
		if len(joiningAddr) == 0:
			return
		else:
			soc = socket.socket()
			soc.connect(joiningAddr)
			message = ["join", self.key, self.address]
			message = dumps(message)
			soc.sendto(message.encode("utf-8"), joiningAddr)
			soc.close()
			
			# updating our successors and predecessor
			data = self.join_queue.get()
			if data[1] == 1:
				self.successor = (data[2][0], data[2][1])
				self.predecessor = (data[2][0], data[2][1])
			
			elif data[1] == 2:
				self.successor = (data[2][0], data[2][1])
				soc = socket.socket()
				soc.connect(self.successor)
				new_message = ["I am your predecessor", 0, self.address]
				new_message = dumps(new_message)
				soc.send(new_message.encode("utf-8"))
				soc.close()
				
				new_data = self.predecessor_queue.get()
				self.predecessor = (new_data[0], new_data[1])
				soc = socket.socket()
				soc.connect(self.predecessor)
				new_message1 = ["I am your successor", 0, self.address]
				new_message1 = dumps(new_message1)
				soc.send(new_message1.encode("utf-8"))
				soc.close()

			self.second_succ_helper()

			# Now it is time to get our share of files
			soc = socket.socket()
			soc.connect(self.successor)
			new_message_2 = ["My files share", self.key, self.predecessor]
			new_message_2 = dumps(new_message_2)
			soc.send(new_message_2.encode("utf-8"))

			# receiving files
			while soc.recv(1024).decode("utf-8") == "continue":
				file_name = soc.recv(1024).decode("utf-8")
				file_path = self.host + "_" + str(self.port) + "/" + file_name
				self.recieveFile(soc, file_path)
				self.files.append(file_name)
			
			soc.close()

			# after joining, starting thread for pinging
			threading.Thread(target = self.pinging, args = ()).start()
			'''
			print("self.addr", self.address)
			print("self.keys", self.key)
			print("self.succ", self.successor, "key:", self.hasher(self.successor[0] + str(self.successor[1])))
			print("self.pred", self.predecessor,"key:", self.hasher(self.predecessor[0] + str(self.predecessor[1])))
			'''
			


	def put(self, fileName):
		file_hash = self.hasher(fileName)
		#print("FILENAME:", fileName, "key:", file_hash)
		message = ["lookup", file_hash, self.address]
		self.look_up(message, file_hash)
		desired_node_recvd = self.join_queue.get()
		desired_node_recvd = desired_node_recvd[2]
		desired_node_address = (desired_node_recvd[0], desired_node_recvd[1])
		
		soc = socket.socket()
		soc.connect(desired_node_address)
		new_message = ["Sending you my file", fileName]
		new_message = dumps(new_message)
		soc.send(new_message.encode("utf-8"))
		
		# this is used because whenever I send above message to a node, it receives the message and calls
		# receiveFile func in HandleConnection. But in this self.node after sending above message, it immediately
		# calls sendFile func which sends the fileSize first, but the receiveFile is not ready in HandleConnection
		# to receive the data because of time difference in func executions, So I used time.sleep to solve this.
		
		time.sleep(1) 
		self.sendFile(soc, fileName)
		soc.close()
		
	def get(self, fileName):
		file_hash = self.hasher(fileName)
		message = ["lookup", file_hash, self.address]
		self.look_up(message, file_hash)
		data = self.join_queue.get()
		node_addr = data[2]
		node_addr = (node_addr[0], node_addr[1])

		soc = socket.socket()
		soc.connect(node_addr)
		new_message = ["get_file", fileName]
		new_message = dumps(new_message)
		soc.send(new_message.encode("utf-8"))
		new_data = soc.recv(1024).decode("utf-8")
		
		if new_data == "None":
			soc.close()
			return None
		else:
			self.recieveFile(soc, fileName)
			soc.close()
			return fileName

		
	def leave(self):
		#print("self.addr for LEAVNG:", self.address)

		# sending files to successor
		new_message = ["I am leaving", "My files", self.files]
		new_message = dumps(new_message)
		soc = socket.socket()
		soc.connect(self.successor)
		soc.send(new_message.encode("utf-8"))
		
		for i in self.files:
			file_path = self.host + "_" + str(self.port) + "/" + i
			time.sleep(1)
			self.sendFile(soc, file_path)
			os.remove(file_path)
		soc.close()

		# telling predecessor that I am leaving and to update yourself accordingly
		message = ["I am leaving", self.successor]
		message = dumps(message)
		soc = socket.socket()
		soc.connect(self.predecessor)
		soc.send(message.encode("utf-8"))
		soc.close()

		# killing my threads
		self.stop = True
		
	def second_succ_helper(self):
		soc = socket.socket()
		soc.connect(self.successor)
		message = ["Second successor"]
		message = dumps(message)
		soc.send(message.encode("utf-8"))
		data = soc.recv(1024).decode("utf-8")
		data = loads(data)
		self.second_successor = (data[0], data[1])
		soc.close()

	def pinging(self):
		while not self.stop:
			check = 0
			time.sleep(0.5)
			for i in range(3):
				#soc.settimeout(0.2)
				try:
					soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					soc.connect(self.successor)
					soc.settimeout(0.2)
					message = ["Pinging", self.address]
					message = dumps(message)
					soc.send(message.encode("utf-8"))
					anything = soc.recv(1024).decode("utf-8")
					soc.close()
					check = 1
					break
				except:
					soc.close()
					check = 0
			if check == 0:
				try:
					# updating my successor to second successor
					self.successor = self.second_successor
					soc = socket.socket()
					soc.connect(self.second_successor)
					message = ["I am your predecessor", 1, self.address]
					message = dumps(message)
					soc.send(message.encode("utf-8"))
					temp = soc.recv(1024).decode("utf-8")
					soc.close()

					# telling my second successor to backup files of failed node
					soc = socket.socket()
					soc.connect(self.successor)
					new_message = ["Node fails, backup files now"]
					new_message = dumps(new_message)
					soc.send(new_message.encode("utf-8"))
					soc.close()

					# setting my new second successor
					self.second_succ_helper()

					# backing up files to successor
					for i in self.files:
						soc = socket.socket()
						soc.connect(self.successor)
						new_message = ["backupfile", [i]]
						new_message = dumps(new_message)
						soc.send(new_message.encode("utf-8"))
						file_path = self.host + "_" + str(self.port) + "/" + i
						time.sleep(0.3)
						self.sendFile(soc, file_path)
						soc.close()
				except:
					pass


	def sendFile(self, soc, fileName):
		fileSize = os.path.getsize(fileName)
		soc.send(str(fileSize).encode('utf-8'))
		soc.recv(1024).decode('utf-8')
		with open(fileName, "rb") as file:
			contentChunk = file.read(1024)
			while contentChunk!="".encode('utf-8'):
				soc.send(contentChunk)
				contentChunk = file.read(1024)

	def recieveFile(self, soc, fileName):
		fileSize = int(soc.recv(1024).decode('utf-8'))
		soc.send("ok".encode('utf-8'))
		contentRecieved = 0
		file = open(fileName, "wb")
		while contentRecieved < fileSize:
			contentChunk = soc.recv(1024)
			contentRecieved += len(contentChunk)
			file.write(contentChunk)
		file.close()

	def kill(self):
		# DO NOT EDIT THIS, used for code testing
		self.stop = True

		
