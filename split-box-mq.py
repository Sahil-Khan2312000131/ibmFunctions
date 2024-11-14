import sys
import time
import requests
# import pymqi
import json 
import concurrent
from concurrent.futures import ThreadPoolExecutor

#Object Storage End Point
object_storage_endpoint = "https://s3.jp-tok.cloud-object-storage.appdomain.cloud/{0}/{1}"
object_storage_read_api_key = "V3-g3BJITajE2eJG51skBqJYzqrcXnRfstrBCdUTzwsw"
object_storage_read_access_token_url = "https://iam.cloud.ibm.com/identity/token"

queue_manager = 'demoqueue'
channel = 'CLOUD.APP.SVRCONN'
host = 'demoqueue-c2f5.qm.eu-de.mq.appdomain.cloud'
queue_name = 'CI_QUEUE'
#port = '32480'
port = '30350'
print("Change to make trigger work!")

# MQ connection options
# conn_info = f'{host}({port})/{channel}'
# conn_options = {
#     pymqi.CMQC.MQCA_Q_MGR_NAME: queue_manager,
#     pymqi.CMQC.MQCACH_CHANNEL_NAME: channel,
# }

#Authorized header for calls to Object Storage Cloud
object_storage_read_headers={
    "Authorization": "Bearer ",
    "ibm-service-instance-id":"ac7d543b-3eda-4333-bfc7-e7bf5622f521"
}

#Processing of object storage data
def process_data(response):
    # print("Response from object store:")
    # print(response.text)
    ans = response.text.split("\n")
    print("Total number of records are ",len(ans)-1)
    data = ans[1: ]
    data_list = []
    for x in data:
        data_list.append(x.split(","))
    headers = ans[0].split(",")
    return headers, data_list

    
#Http calls for actions:
def make_action_http_call(url, data):
    authorization_header = "Basic Y2lhcHA6QmtHYzlKbHpkbE1aWklQVkk5RGtUcklkZ0J5eVhoSVFFRXhrZlIwcnlRalU="

    headers = {"ibm-mq-rest-csrf-token": "token", "Content-Type": "application/json;charset=utf-8","Authorization": authorization_header}
    print(data)
    print("\n")
    print(json.dumps(data))
    response = requests.post(url, json = json.dumps(data), headers=headers, timeout = 5)  #Expliclity setting timeout
    return response
    
    
def generate_object_storage_read_api_access_token():
    
    read_access_token_requestBody = "grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey={}".format(object_storage_read_api_key) 
    read_access_token_response = requests.post(object_storage_read_access_token_url, data = read_access_token_requestBody)
    json_response = json.loads(read_access_token_response.content)
    
    object_storage_read_headers["Authorization"]+=json_response['access_token']    

def main(dict):

    #need to fetch bucket key and document key from dict itself
    print("Bucket name is ", dict['bucket'])
    print("Endpoint is ", dict['endpoint'])
    print("Bucket key is ", dict['key'])
    
    #Don't use end points because it's coming for private URL 
    bucket_name = dict['bucket']
    bucket_key = dict['key']

    #Refreshing Read Access Token
    generate_object_storage_read_api_access_token()
    
    #Can write another action for fetching Access token via bucket name
    #Above response can contain public end point as well access token

    response = requests.get(object_storage_endpoint.format(bucket_name, bucket_key), headers = object_storage_read_headers)
    
    #Ingest received response in MQ
    data_header, data_list = process_data(response)
    print("total Number of records for boxes ", len(data_list))
    json_data = [{"message": data, "retryCount": 3, "messageType": "FOUR_WHEELER"} for data in data_list]

    # try:
        # Connect to MQ
        # qmgr = pymqi.QueueManager(None)
        # qmgr.connectTCPClient(queue_manager, conn_info, conn_options)

        # # Open the queue
        # queue = pymqi.Queue(qmgr, queue_name)

        # # Put the message on the queue
        # put_opts = pymqi.PMO()
        
    queue_url = 'https://web-demoqueue-c2f5.qm.eu-de.mq.appdomain.cloud/ibmmq/rest/v3/messaging/qmgr/demoqueue/queue/CI_QUEUE/message'
    for item in json_data:
        try:
            response = make_action_http_call(queue_url, item)
            print(response)
        except requests.exceptions.RequestException as e:
            print("Error in ingesting input via HTTP: " + str(e))
        # queue.put(item, None, None, put_opts)
    print ("data ingested successfully to MQ")
    # return {"result": "Message sent successfully."}

    # except Exception as e:
    #     return {"error in ingesting input to MQ ": str(e)}

    # finally:
    #     # Disconnect from MQ
    #     queue.close() if 'queue' in locals() else None
    #     qmgr.disconnect() if 'qmgr' in locals() else None
    
    print('Process completed')
    
    return {}
