#!/usr/bin/env python2.7

# Ik hoop zo erg dat iemand deze code verbouwt, en dat het efficient wordt. Dat is het momenteel niet.

import sys
import zmq
import simplejson as serializer
import time
from ctx import ctx
from gzip import GzipFile
from cStringIO import StringIO
from datetime import datetime
import psycopg2

conn = psycopg2.connect("dbname='gtfs_kv7'")

ZMQ_PUBSUB_KV8 = "tcp://83.98.158.170:7817"
ZMQ_PUBSUB_KV8_ANNOTATE = "tcp://127.0.0.1:7855"

# Initialize a zeromq CONTEXT
context = zmq.Context()
sys.stderr.write('Setting up a ZeroMQ SUB: %s\n' % (ZMQ_PUBSUB_KV8))
subscribe_kv8 = context.socket(zmq.SUB)
subscribe_kv8.connect(ZMQ_PUBSUB_KV8)
subscribe_kv8.setsockopt(zmq.SUBSCRIBE, '/GOVI/KV8')

sys.stderr.write('Setting up a ZeroMQ REP: %s\n' % (ZMQ_PUBSUB_KV8_ANNOTATE))
client_annotate = context.socket(zmq.REP)
client_annotate.bind(ZMQ_PUBSUB_KV8_ANNOTATE)

# Set up a poller
poller = zmq.Poller()
poller.register(subscribe_kv8, zmq.POLLIN)
poller.register(client_annotate, zmq.POLLIN)

# Cache
actuals = {}
stop_times = {}
destinations = {}

while True:
    socks = dict(poller.poll())

    if socks.get(subscribe_kv8) == zmq.POLLIN:
        multipart = subscribe_kv8.recv_multipart()
        content = GzipFile('','r',0,StringIO(''.join(multipart[1:]))).read()
        c = ctx(content)
        if 'DATEDPASSTIME' in c.ctx:
            for row in c.ctx['DATEDPASSTIME'].rows():
                tripid = '_'.join([row['DataOwnerCode'],  row['LinePlanningNumber'], row['LocalServiceLevelCode'], row['JourneyNumber'], row['FortifyOrderNumber']])
                if tripid not in stop_times:
                    stop_times[tripid] = {}
                    cur = conn.cursor()
                    cur.execute('select stop_sequence, arrival_time, departure_time from gtfs_stop_times where trip_id = %s;', [tripid])
                    dbrows = cur.fetchall()
                    for dbrow in dbrows:
                        stop_times[tripid][int(dbrow[0])] = (dbrow[1], dbrow[2])

                if tripid not in destinations:
                    destinations[tripid] = None
                    cur = conn.cursor()
                    cur.execute('select route_short_name, trip_headsign from gtfs_trips, gtfs_routes where gtfs_trips.route_id = gtfs_routes.route_id and trip_id = %s;', [tripid])
                    dbrows = cur.fetchall()
                    if len(dbrows) > 0:
                        destinations[tripid] = (dbrows[0][0], dbrows[0][1])

                if destinations[tripid] is not None:
                    row['LinePublicNumber'] = destinations[tripid][0]
                    row['DestinationName'] =  destinations[tripid][1]
                else:
                    row['DestinationName'] = ''
                    row['LinePublicNumber'] = ''

                fid = tripid + '_' + row['UserStopOrderNumber']
                
                if tripid in stop_times:
                    row['TargetDepartureTime'], row['TargetArrivalTime'] = stop_times[tripid][int(row['UserStopOrderNumber'])]
                else:
                    row['TargetDepartureTime'], row['TargetArrivalTime'] = row['ExpectedDepartureTime'], row['ExpectedArrivalTime']

                hours, minutes, seconds = row['ExpectedDepartureTime'].split(':')
                expected = ((int(hours) * 60) + int(minutes)) * 60 + int(seconds)
                hours, minutes, seconds = row['TargetDepartureTime'].split(':')
                target = ((int(hours) * 60) + int(minutes)) * 60 + int(seconds)
                row['TimeOut'] = max(expected, target) + 120 

                if row['TimingPointCode'] not in actuals or fid not in actuals[row['TimingPointCode']]:
                    actuals[row['TimingPointCode']] = {fid: row}
                else:
                    actuals[row['TimingPointCode']][fid] = row

                print row['TimingPointCode']

    elif socks.get(client_annotate) == zmq.POLLIN:
        now = datetime.now()
        timeout = ((int(now.hour) * 60) + int(now.minute)) * 60 + int(now.second)

        array = client_annotate.recv_json()
        output = '<DRIS_55><License>http://openov.nl/license/GOVI-1.0-TravelInfo</License><Request>'
        for tpc in array:
            if tpc in actuals:
                output += '<TimingPoint><TimingPointCode>%s</TimingPointCode>' % (tpc)
                print len(actuals[tpc].items())
                for fid, trip in actuals[tpc].items():
                    if timeout < trip['TimeOut']:
                        output += '<Trip><OperatingDate>%(OperationDate)s</OperatingDate><DataOwnerCode>%(DataOwnerCode)s</DataOwnerCode><LinePlanningNumber>%(LinePlanningNumber)s</LinePlanningNumber><LinePublicNumber>%(LinePublicNumber)s</LinePublicNumber><DestinationName>%(DestinationName)s</DestinationName><JourneyNumber>%(JourneyNumber)s</JourneyNumber><FortifyOrderNumber>%(FortifyOrderNumber)s</FortifyOrderNumber><TargetArrivalTime>%(TargetArrivalTime)s</TargetArrivalTime><ExpectedArrivalTime>%(ExpectedArrivalTime)s</ExpectedArrivalTime><TargetDepartureTime>%(TargetDepartureTime)s</TargetDepartureTime><ExpectedDepartureTime>%(ExpectedDepartureTime)s</ExpectedDepartureTime><TripStopStatus>%(TripStopStatus)s</TripStopStatus></Trip>' % trip
                    else:
                        tripid = '_'.join(fid.split('_')[:-1])
                        del stop_times[tripid]
                        del destinations[tripid]
                        del actuals[tpc][fid]

                output += '</TimingPoint>'
        output += '</Request></DRIS_55>'
            
        client_annotate.send(output)
