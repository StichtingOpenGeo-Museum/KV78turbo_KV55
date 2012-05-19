import uwsgi
import zmq
import simplejson

ZMQ_PUBSUB_KV8_ANNOTATE = "tcp://127.0.0.1:7855"

# Initialize a zeromq CONTEXT
context = zmq.Context()
client_annotate = context.socket(zmq.REQ)
client_annotate.connect(ZMQ_PUBSUB_KV8_ANNOTATE)

# from const import ZMQ_KV78DEMO
COMMON_HEADERS = [('Content-Type', 'application/xml'), ('Access-Control-Allow-Origin', '*'), ('Access-Control-Allow-Headers', 'Requested-With,Content-Type')]

def KV55(environ, start_response):
    url = environ['PATH_INFO'][1:]
    if len(url) > 0 and url[-1] == '/':
        url = url[:-1]
        
    arguments = url.split(',')
    try:
        for x in arguments:
            int(x)
    except:
        reply = '<DRIS_55 />'
        start_response('500 Internal Server Error', COMMON_HEADERS + [('Content-length', str(len(reply)))])
        return reply
    
    client_annotate.send_json(arguments)
    reply = client_annotate.recv()
    
    start_response('200 OK', COMMON_HEADERS + [('Content-length', str(len(reply)))])
    return reply

uwsgi.applications = {'': KV55}
