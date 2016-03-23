import queue
import json
import hashlib
import base64
from collections import OrderedDict
from operator import itemgetter

class MessageService:

	def __init__(self):
		#list of output queues 
		self.msg_queue_list = [queue.Queue(), queue.Queue(),queue.Queue(),queue.Queue(),queue.Queue()]
		#list of messages that are part of a sequence that are waiting to be enqueued 
		self.sequenced_msgs= []
		#dictionary to hold the seqence name, queue to route the rest of the messages to and the last message number that was enqueued
		self.seq_dict= {}
	
	@staticmethod 
	def encode(s):
		encoded_string8= s.encode('utf-8')
		encoded_string64 = base64.encodestring(encoded_string8)
		hashed_string =hashlib.sha256(encoded_string64).hexdigest()
		return hashed_string

	#defines and applies dispatch rules to message  
	@staticmethod
	def dispatch(msg):
		if '_special' in msg:
			return 0 
		elif 'hash' in msg: 
			return 1
		else:
			for k, v in msg.items():
				if not k.startswith("_"):
					if isinstance (v, str):
						if 'muidaQ' in v:
							return 2 
					elif isinstance(v,int):
						return 3 
		return 4

	#defines and applies transformation rules to a message 
	@staticmethod
	def transform(msg):
		for k , v in msg.items():
			if not k.startswith("_"):
				if isinstance (v, str):
					if 'Qadium' in v:
						msg[k] = v[::-1]
				elif isinstance(v,int):
					msg[k] = ~ v
			if '_hash' in k:
				msg['hash'] = encode(v)	
		return msg 
	
	#handles sequenced messages. Enqueuing messages of a sequence in order by storing messages in a list until they are ready for processing 
	def enqueue_sequence(self, msg):
		seq_name = msg['_sequence']
		#check if first message in sequence 
		if msg['_part'] == 0:
			#apply dispatch rules
			qnum = self.dispatch(msg)
			self.seq_dict = { seq_name: {'route': qnum, 'last': 0}}
			#enqueue message to appropriate queue
			self.msg_queue_list[qnum].put(json.dumps(msg))
		#check if message is next message to send to output queue
		else:
			self.sequenced_msgs.append(msg)

		#sort list to prepare for  search 
		self.sequenced_msgs.sort(key = itemgetter('_sequence', '_part'), reverse = True)
		#search list for next messages in sequence
		for i in reversed(range(len(self.sequenced_msgs))):
			#check if sequence name is in the dict holding the routing informaiton
			if seq_name in self.seq_dict:
				#check sequence name and check part number is next part to output 
				if self.sequenced_msgs[i].get('_sequence') == seq_name and self.sequenced_msgs[i].get('_part') == self.seq_dict[seq_name]['last']+1:
					self.msg_queue_list[self.seq_dict[seq_name]['route']].put(json.dumps(self.sequenced_msgs.pop(i))) #enqueue
					self.seq_dict[seq_name]['last'] += 1 #update last message enqueued in sequence 
	
	
	def enqueue(self, msg):
		##read in JSON message 
		message = json.loads(msg, object_pairs_hook=OrderedDict)
		#apply transformation rules
		transformed_msg = self.transform(message)

		if '_sequence' in transformed_msg:
			self.enqueue_sequence(transformed_msg)
		else:
			#apply dispatch rules 
			qnum = self.dispatch(transformed_msg)
			qmsg= json.dumps(transformed_msg)
			#enqueue message to appropriate queue
			self.msg_queue_list[qnum].put(qmsg)

	def next(self, qnum):
		return self.msg_queue_list[qnum].get(block=False)
		

def get_message_service():
	"""Returns a new, "clean" Q service."""
	return MessageService()