# Dimitris Pattas AM:2331, email:dimitris.pattas@gmail.com
# se sunergasia me Giwrgo Kymparidi AM:2279

# command-line   python twoPhaseCommit.py (node id) (node ip to connect) (:) (port)	
# node id = 0 or 1 or 2
# example
# python twoPhaseCommit.py 0 2331 							(ip node 1 : 10.7.4.46)
# python twoPhaseCommit.py 1 10.7.4.46:2331 				(ip node 2 : 10.7.4.49)
# python twoPhaseCommit.py 2 10.7.4.46:2331 				(ip node 3 : 10.7.4.48)


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
import random

global file
links = []
votes = []
messageState = ''
participantVote = ''
counter2pc = 0


 
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
	global  timeOutFlag 
	acks = 0
	connected = False
	receivedMessageBuffer = []
	decision = ''
	state = 'INIT'

	def __init__(self, factory, peer_type):
		self.pt = peer_type
		self.factory = factory
		

	def connectionMade(self):

		if self.isCoordinator():
			self.setLinks();
			self.connected = True
			if self.isSystemInConnecting():
				self.initVotesTable()
				self.openFile()	
				self.sendVoteRequest()
		else:
			self.openFile()
			self.connected = True
			
	def sendUpdate(self, message):	
		print 'Send ' + message
		message = message+','+self.pt
		try:
				if self.isCoordinator():
					for link in links:
						link.transport.write(message)
				else:
					self.transport.write(message)
		except Exception, ex1:
			print "Exception trying to send: ", ex1.args[0]



	def dataReceived(self, data):
		print 'Received '+ data
		if  self.isCoordinator():
			self.editTheMessage(data)
			self.decisionByCoordinator()
		else:
			self.editTheMessage(data)

	def sendVoteRequest(self):
		global counter2pc
		votes[0] = ''
		votes[1] = ''
		votes[2] = ''
		if counter2pc < 5:
			self.writeToFile('START_2PC')
			self.sendUpdate('VOTE_REQUEST')
			self.setVote('VOTE_COMMIT', 0)
			counter2pc += 1
			reactor.callLater(2, self.timeOut)
			reactor.callLater(3, self.sendVoteRequest)

	def timeOut(self):
		if self.isCoordinator():
			if self.allVotesHaveBeenCollected():
				return
			else:
				print 'timeOut'
				self.writeToFile('GLOBAL_ABORT')
				self.sendUpdate('GLOBAL_ABORT')
		else:
			if self.decision == '':
				self.writeToFile('GLOBAL_ABORT')
				self.state = 'ABORT'

	def decisionByCoordinator(self):
		if self.allVotesHaveBeenCollected():
			if self.isAllVotesCommit():
				self.writeToFile('GLOBAL_COMMIT')
				self.sendUpdate('GLOBAL_COMMIT')
			else:
				self.writeToFile('GLOBAL_ABORT')
				self.sendUpdate('GLOBAL_ABORT')
			 
	def editTheMessage(self, data):
		m = self.parseRealMessage(data)
		n = self.parseSenderName(data)
		if self.isCoordinator():
			index = self.getSendersId(n)
			votes[index] = m
		else:
			messageState = m
			if messageState == 'VOTE_REQUEST':
				self.writeToFile('INIT')
				self.state = 'INIT'
				self.decision = ''
				self.voteByParticipant()
			else:
				self.decision = messageState
				if self.state != 'ABORT':
					self.writeToFile(messageState)

	
	def voteByParticipant(self):
		randomNumber = random.randint(0, 9)
		if randomNumber >= 4:
			participantVote = 'VOTE_COMMIT'
		else:
			participantVote = 'VOTE_ABORT'
		self.writeToFile(participantVote)
		if participantVote == 'VOTE_ABORT':
			self.state = 'ABORT'
			self.writeToFile('GLOBAL_ABORT')
		else:
			self.state = 'READY'
		self.sendUpdate(participantVote)
		
		# set timeout 
		reactor.callLater(1, self.timeOut)

	def getSendersId(self, senderName):
		if senderName == 'participant1':
			return 1
		elif senderName == 'participant2':
			return 2

				
	def isCoordinator(self):
		if self.pt == 'coordinator':
			return True;
		return False;

	def setLinks(self):
		links.append(self)

	def allVotesHaveBeenCollected(self):
		if ((votes[1] != '') and (votes [2] != '')):
			return True
		return False

	def initVotesTable(self) :
		votes.append('')
		votes.append('')
		votes.append('')

	def setVote(self, vote, index):
		votes[index] = vote

	def isAllVotesCommit(self):
		for i in range(0,3):
			if votes[i] != 'VOTE_COMMIT':
				return False
		return True

	def clearVotesTable(self):
		for i in range(0,3):
			votes[i] = ''

	def openFile(self):
		global file
		p = ''
		if self.pt == 'coordinator':
			p = '0'
		elif self.pt == 'participant1':
			p = '1'
		else:
			p = '2'
		name = 'log-' + p
		file = open(name, 'w')
		file.flush()
		return

	def writeToFile(self, state):
		file.write(state + '\n')
		file.flush()

	def parseRealMessage(self,data):
		m, n = data.split(',') 
		return m	

	def parseSenderName(self,data):
		m, n = data.split(',') 
		return n

	def isSystemInConnecting(self):
		if len(links) > 1:
			return True
		return False

	def connectionLost(self, reason):
		print "Disconnected"
		if self.pt == 'client':
			self.connected = False
			self.done()

	def done(self):
		self.factory.finished(self.acks)

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
		return protocol

if __name__ == '__main__':
	peer_type, address = parse_args()

	# coordinator-------------------------------------
	if peer_type == '0':
		factory = PeerFactory('coordinator', 'listen')
		reactor.listenTCP(address[1], factory)
	
	# participants------------------------------------
	elif peer_type == '1':
		factory = PeerFactory('participant1', 'connect')
		host, port = address
		print "Connecting to host " + host + " port " + str(port)
		reactor.connectTCP(host, port, factory)

	elif peer_type == '2':
		factory = PeerFactory('participant2', 'connect')
		host, port = address
		print "Connecting to host " + host + " port " + str(port)
		reactor.connectTCP(host, port, factory)

	reactor.run()
