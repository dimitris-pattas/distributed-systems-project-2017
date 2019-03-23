# Dimitris Pattas AM:2331, email:dimitris.pattas@gmail.com

# command-line   python peer-solutionFinal.py (node id) (node ip to connect) (:) (port)	
# node id = 1 or 2 or 3
# example
# python peer-solutionFinal.py 1 2331 							(ip node 1 : 10.7.4.46)
# python peer-solutionFinal.py 2 10.7.4.46:2331 				(ip node 2 : 10.7.4.49)
# python peer-solutionFinal.py 3 10.7.4.49,10.7.4.46:2331 		(ip node 3 : 10.7.4.48)


# Event-driven code that behaves as either a client or a server
# depending on the argument.  When acting as client, it connects 
# to a server and periodically sends an update message to it.  
# Each update is acked by the server.  When acting as server, it
# periodically accepts messages from connected clients.  Each
# message is followed by an acknowledgment.
#
# Tested with Python 2.7.8 and Twisted 14.0.2
#
import optparse
import collections
import sys
import operator


from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet import reactor
import time

global file
links = []
lamportClocks = [] 
listOfMessages = []

def parse_args():
	usage = """usage: %prog [options] [client|server] [hostname]:port

	python peer.py server 127.0.0.1:port """

	parser = optparse.OptionParser(usage)

	_, args = parser.parse_args()

	if len(args) != 2:
		print parser.format_help()
		parser.exit()

	peertype, addresses = args

	def parse_address(addr):
		if ':' not in addr:
			host = '127.0.0.1'
			port = addr
        	else:
			host, port = addr.split(':', 1)

		if not port.isdigit():
			parser.error('Ports must be integers.')
		if ',' in host:
			hostList = host.split(',') 
			return hostList[0], hostList[1], int(port)
		return host, int(port)

	return peertype, parse_address(addresses)


class Peer(Protocol):

	acks = 0
	connected = False
	action = ''
	peerId = -1
	myMsgsCounter = 0
	processId = -1
	NUMBER_OF_MESSAGES = 20
	firstDataReceived = True

	receivedMessageBuffer = []

	def __init__(self, factory, peer_type):
		self.pt = peer_type
		self.factory = factory

	def connectionMade(self):
		links.append(self)
		self.setPeerId( len(links) - 1 )
		if self.peerId == 0:
			self.initLamportClock()
		self.initProcessId()
		if self.isSystemInConnecting():
			self.sendUpdate()

	def sendUpdate(self):
		self.myMsgsCounter += 1
		self.kickMyLamportClock()
		print 'send Message'
		myLamportClock = self.getMyLamportClock()
		msg = '<start> <msg-' + str(self.myMsgsCounter) + '> from ' + self.pt + ' with peerId: ' + str(self.peerId) + ', ' + str(myLamportClock) + ', ' + str(self.processId)+' <end> |'
		m = '<msg-' + str(self.myMsgsCounter) + '> from ' + self.pt + ' with peerId: ' + str(self.peerId)
		print m  + ', ' + str(myLamportClock) + ', ' +  str(self.processId)  
		listOfMessages.append( Message(m, myLamportClock, self.processId) )
		try:
			
				for link in links:
					link.transport.write(msg)
		except Exception, ex1:
			print "Exception trying to send: ", ex1.args[0]
		if self.connected == True:
			if self.thereIsOtherMessage():
						reactor.callLater(2, self.sendUpdate)

	def sendAck(self):
		print 'send Ack'	
		self.ts = time.time()
		try:
			for link in links:
				link.transport.write('<start> <Ack> from ' + self.pt + ' with  '+str(self.peerId) +', ' + str(self.getMyLamportClock())+' <end> |' )
		except Exception, e:
			print e.args[0]

	def dataReceived(self, data):
		self.addMessageInBuffer(data)
		self.editTheMessages()
		self.sortListOfMessages()
		self.deliver()
		

	def addMessageInBuffer(self, data):
		#toulaxiston ena minima
		size = len(self.receivedMessageBuffer)
		if size > 0:
			if not('<end>' in self.receivedMessageBuffer[size -1] ):
				if '|' in data:
					msg = data.split(' |')
					flag = True
					for m in msg:
						if flag == True:
							self.receivedMessageBuffer[size - 1] = self.receivedMessageBuffer[size - 1] + m
							flag = False
						elif flag == False and m != '':
							self.receivedMessageBuffer.append(m)
				else:
					self.receivedMessageBuffer[size - 1] = self.receivedMessageBuffer[size - 1] + data	
		else:
			if '|' in data:
					msg = data.split(' |')
					for m in msg:
						if m != '':
							self.receivedMessageBuffer.append(m)
			else:
				self.receivedMessageBuffer.append(m)
		

	def getMessageByBuffer(self):
		if len(self.receivedMessageBuffer) > 0:
			if '<end>' in self.receivedMessageBuffer[0]:
				msg = self.receivedMessageBuffer[0]
				msg = msg.replace('<start> ','')
				msg = msg.replace('<end>','')
				self.receivedMessageBuffer.pop(0)
				return msg
			else:
				return None
		else:
			return None

	
	def editTheMessages(self):
		while True:
			msg = self.getMessageByBuffer()
			if msg == None:
				break
			if self.isMessageAck(msg):
				print 'receive Ack'
				print msg
				self.acks += 1
				lcFromOtherNode = self.parseLamportClockFromAck(msg)
				lcId = self.findSenderLamportClockId()
				self.updateLamportClock(lcFromOtherNode, lcId)
			else:
				print 'receive Message'
				print msg
				lcFromOtherNode = self.parseLamportClockFromMsg(msg)
				lcId = self.findSenderLamportClockId()
				self.updateLamportClock(lcFromOtherNode, lcId)
				m = self.parseRealMessage(msg)
				pid  = self.parseProcessId(msg)
				listOfMessages.append( Message(m, lcFromOtherNode, pid) )
				self.kickMyLamportClock()
				self.sendAck()
				
	def initLamportClock(self):
		for i in range(0,3):
			lamportClocks.append(1.0)

	def isMessageAck(self,data):
		if '<Ack> from' in data:
			return True
		return False

	def parseLamportClockFromAck(self, data):
		a, l = data.split(', ')
		return float(l)

	def parseLamportClockFromMsg(self,data):
		m, l, p = data.split(', ') 
		return float(l)

	def parseRealMessage(self,data):
		m, l, p = data.split(', ') 
		return m

	def parseProcessId(self, data):
		m, l, p =  data.split(', ')
		return int(p)

	def findSenderLamportClockId(self):
		if self.pt == 'node1':
			if self.peerId == 0:
				return 1
			elif self.peerId == 1:
				return 2
		elif self.pt == 'node2':
			if self.peerId == 0:
				return 0
			elif self.peerId == 1:
				return 2
		elif self.pt == 'node3':
			if self.peerId == 0:
				return 1
			elif self.peerId == 1:
				return 0

	def updateLamportClock(self, lcFromOtherNode, lcId):
		nodeId = self.getMyNodeId()
		lamportClocks[nodeId] = max(lamportClocks[nodeId], lcFromOtherNode) + 1.0
		#print str(lamportClocks[nodeId])
		lamportClocks[lcId] = lcFromOtherNode
		self.printLamportClockTable()

	def kickMyLamportClock(self):
		print ' kick my clock' 
		nodeId = self.getMyNodeId()
		lamportClocks[nodeId] = lamportClocks[nodeId] + 1.0
		self.printLamportClockTable()

	def getMyLamportClock(self):
		nodeId = self.getMyNodeId()
		return lamportClocks[nodeId]

	def getMyNodeId(self):
		if self.pt == 'node1':	
			return 0
		elif self.pt == 'node2':
			return 1
		else:
			return 2

	def printLamportClockTable(self):
		for i in range(0,len(lamportClocks)):
			print ' lamportClocks['+str(i)+'] = '+str(lamportClocks[i])


	def sortListOfMessages(self):
		copylist = sorted(listOfMessages, key=lambda e: (e.getTimeStamp(), e.getPid()))
		for i in range(0,len(listOfMessages)):
				listOfMessages.pop()
		for i in copylist:
				listOfMessages.append(i)

	def deliver(self):
		notDeliverMessageList = []
		for i in listOfMessages:
			flag = 0
			for nodeClock in lamportClocks:
				if i.getTimeStamp() <= nodeClock:
					flag += 1
			if flag == 3:
				msg = i.getMessage()+' '+str(i.getTimeStamp())+' '+str(i.getPid())
				self.writeMessages(msg)
			else:
				notDeliverMessageList.append(i)
		for i in range(0,len(listOfMessages)):
			listOfMessages.pop()
		for i in notDeliverMessageList:
			listOfMessages.append(i)

	def isSystemInConnecting(self):
		if len(links) > 1:
			return True
		return False

	def  thereIsOtherMessage(self):
		if self.myMsgsCounter < self.NUMBER_OF_MESSAGES:
			return True
		return False

	def writeMessages(self, data):
		global file
		if self.firstDataReceived == True:
			p = ''
			if self.pt == 'node1':
				p = '1'
			elif self.pt == 'node2':
				p = '2'
			else:
				p = '3'
			name = 'delivered-messages-' + p
			file = open(name, 'w')
			for link in links:
				link.firstDataReceived =False
		file.write(data + '\n')
		file.flush()

	def connectionLost(self, reason):
		print "Disconnected"
		if self.pt == 'client':
			self.connected = False
			self.done()

	def done(self):
		self.factory.finished(self.acks)

	def setAction(self,action):
		self.action = action

	def setPeerId(self,peerId):
		self.peerId = peerId

	def initProcessId(self):
		self.processId = self.getMyNodeId()

class Message:
	msg = ''
	timeStamp = -1.0
	processId = -1
	sender = ''
	safeToDeliver = False

	def __init__(self, msg, timeStamp, pid):
		self.msg = msg
		self.timeStamp = timeStamp
		self.processId = pid

	def getMessage(self):
		return self.msg

	def getTimeStamp(self):
		return self.timeStamp

	def getPid(self):
		return self.processId

	def setState(self, state):
		self.safeToDeliver = state

	def getState(self):
		return safeToDeliver

class PeerFactory(ClientFactory):

	def __init__(self, peertype, fname):
		print '@__init__'
		self.pt = peertype
		self.acks = 0
		self.fname = fname
		self.records = []

	def finished(self, arg):
		self.acks = arg
		self.report()

	def report(self):
		print 'Received %d acks' % self.acks

	def clientConnectionFailed(self, connector, reason):
		print 'Failed to connect to:', connector.getDestination()
		self.finished(0)

	def clientConnectionLost(self, connector, reason):
		print 'Lost connection.  Reason:', reason

	def startFactory(self):
		print "@startFactory"
		if self.pt == 'server':
			self.fp = open(self.fname, 'w+')

	def stopFactory(self):
		print "@stopFactory"
		if self.pt == 'server':
			self.fp.close()

	def buildProtocol(self, addr):
		print "@buildProtocol"
		protocol = Peer(self, self.pt)
		protocol.setAction(self.fname)
		return protocol

if __name__ == '__main__':
	peer_type, address = parse_args()

	if peer_type == '1':
		factory = PeerFactory('node1', 'listen')
		reactor.listenTCP(address[1], factory)
	
	elif peer_type == '2':
		
		factory2 = PeerFactory('node2', 'connect')
		host, port = address
		print "Connecting to host " + host + " port " + str(port)
		reactor.connectTCP(host, port, factory2)

		factory1 = PeerFactory('node2', 'listen')
		reactor.listenTCP(address[1], factory1)

	elif peer_type == '3':
		
		host1, host2, port = address
		factory = PeerFactory('node3', 'connect')
		print "Connecting to host " + host1 + " port " + str(port)
		reactor.connectTCP(host1, port, factory)
		time.sleep(2)
		factory = PeerFactory('node3', 'connect')
		print "Connecting to host " + host2 + " port " + str(port)
		reactor.connectTCP(host2, port, factory)

	reactor.run()
