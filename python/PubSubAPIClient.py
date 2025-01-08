import grpc
import requests
import threading
import io
import pubsub_api_pb2 as pb2
import pubsub_api_pb2_grpc as pb2_grpc
import avro.schema
import avro.io
import time
import certifi
import json
import os
from dotenv import load_dotenv

load_dotenv()

class PubSubAPIClient():
    def __init__(self):
        self.semaphore = threading.Semaphore(1)
        self.latest_replay_id = None

    def login(self):    
        username = os.getenv('SALESFORCE_USERNAME')
        password = os.getenv('SALESFORCE_PASSWORD')
        security_token = os.getenv('SALESFORCE_SECURITY_TOKEN')
        url = os.getenv('SALESFORCE_URL')
        headers = {'content-type': 'text/xml', 'SOAPAction': 'login'}
        xml = f"""
        <soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/'
            xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'
            xmlns:urn='urn:partner.soap.sforce.com'>
            <soapenv:Body>
                <urn:login>
                    <urn:username><![CDATA[{username}]]></urn:username>
                    <urn:password><![CDATA[{password}{security_token}]]></urn:password>
                </urn:login>
            </soapenv:Body>
        </soapenv:Envelope>
        """
        res = requests.post(url, data=xml, headers=headers, verify=False)
        #Optionally, print the content field returned
        print(res.content)
    
    def get_authmetadata(self):
        sessionid = '00DVG000002O7lZ!AQEAQKBGHgrrGeQDSdFR9ab6H5fwfzaQKJ8TYKoVmE_jAldCBckDVhNmqJWsqU7LyGp9CO5v0YOIDXZLleSB70gV3KO_TZ_m'
        instanceurl = 'https://amarok--fullcopy.sandbox.my.salesforce.com'
        tenantid = '00DVG000002O7lZ2AS'
        return (
            ('accesstoken', sessionid),
            ('instanceurl', instanceurl),
            ('tenantid', tenantid)
        )
    
    def fetchReqStream(self, topic):
        while True:
            self.semaphore.acquire()
            yield pb2.FetchRequest(
                topic_name = topic,
                replay_preset = pb2.ReplayPreset.LATEST,
                num_requested = 1)
    
    def decode(self, schema, payload):
        schema = avro.schema.parse(schema)
        buf = io.BytesIO(payload)
        decoder = avro.io.BinaryDecoder(buf)
        reader = avro.io.DatumReader(schema)
        ret = reader.read(decoder)
        return ret

    def receive_message(self, stub):
        mysubtopic = "/data/ChangeEvents" #"/data/CaseChangeEvent"
        print('Subscribing to ' + mysubtopic)
        substream = stub.Subscribe(self.fetchReqStream(mysubtopic),
                metadata=self.get_authmetadata())
        for event in substream:
            if event.events:
                self.semaphore.release()
                print("Number of events received: ", len(event.events))
                payloadbytes = event.events[0].event.payload
                schemaid = event.events[0].event.schema_id
                schema = stub.GetSchema(
                        pb2.SchemaRequest(schema_id=schemaid),
                        metadata=self.get_authmetadata()).schema_json
                decoded = self.decode(schema, payloadbytes)
                print("Got an event!", json.dumps(decoded))
            else:
                print("[", time.strftime('%b %d, %Y %l:%M%p %Z'),
                "] The subscription is active.")
            latest_replay_id = event.latest_replay_id
  
    def __call__(self):    
        with open(certifi.where(), 'rb') as f:
            creds = grpc.ssl_channel_credentials(f.read())
        with grpc.secure_channel('api.pubsub.salesforce.com:7443', creds) as channel:
            # All of the code in the rest of the tutorial will go inside this block.
            # Make sure that the indentation of the new code you add starts from this comment’s block.
            #self.login()
            stub = pb2_grpc.PubSubStub(channel)
            self.receive_message(stub)

PubSubAPIClient()()

