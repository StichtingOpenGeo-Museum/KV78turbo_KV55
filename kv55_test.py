import zmq
import sys

ZMQ_PUBSUB_KV8_ANNOTATE = "tcp://127.0.0.1:7855"

# Initialize a zeromq CONTEXT
context = zmq.Context()

sys.stderr.write('Setting up a ZeroMQ REQ: %s\n' % (ZMQ_PUBSUB_KV8_ANNOTATE))
client_annotate = context.socket(zmq.REQ)
client_annotate.connect(ZMQ_PUBSUB_KV8_ANNOTATE)

client_annotate.send_json([sys.argv[1]])
print client_annotate.recv()
