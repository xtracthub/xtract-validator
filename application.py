import datetime
import json
import os
import requests
import time
import boto3
from flask import abort, Flask, request
from globus_sdk import AccessTokenAuthorizer, TransferClient, TransferData

application = Flask(__name__)
eb_url = "http://xtractv1-env-2.p6rys5qcuj.us-east-1.elasticbeanstalk.com"


@application.route("/")
def index():
    return "hello world"


@application.route("/validate", methods=["POST"])
def validate():
    params = request.json
    crawl_id = params["crawl_id"]
    globus_eid = params["globus_eid"]
    transfer_token = params["transfer_token"]
    source_destination = params["source_destination"]
    dataset_info = params["dataset_info"] # To be implemented later

    client = boto3.client('sqs',
                          aws_access_key_id=os.environ["aws_access"],
                          aws_secret_access_key=os.environ["aws_secret"], region_name='us-east-1')

    try:
        response = client.get_queue_url(
            QueueName=f'validate_{crawl_id}',
            QueueOwnerAWSAccountId=os.environ["aws_account_id"])
    except: # Add SQS.Client.exceptions.QueueDoesNotExist error
        abort(400, "Invalid crawl ID")

    try:
        authorizer = AccessTokenAuthorizer(transfer_token)
        tc = TransferClient(authorizer=authorizer)
    except:  # Add exception
        abort(400, "Invalid transfer token")

    crawl_queue = response["QueueUrl"]

    date = datetime.datetime.now()
    file_name = date.strftime("%m_%d_%Y-%H_%M_%S") + ".txt"

    try:
        with open(file_name, "w") as f:

            while True:
                sqs_response = client.receive_message(
                    QueueUrl=crawl_queue,
                    MaxNumberOfMessages=1, # To be toggled
                    WaitTimeSeconds=1)

                if "Messages" not in sqs_response:
                    # xtract_status = requests.get(f"{eb_url}/get_extract_status", json={"crawl_id": crawl_id})
                    # print("HERE")
                    # print(xtract_status.content)
                    # xtract_content = json.loads(xtract_status.content)
                    # # print(xtract_content)
                    #
                    # if xtract_content["IDLE"] == 0 and xtract_content["PENDING"] == 0:
                        break

                del_list = []

                for message in sqs_response["Messages"]:
                    message_body = message["Body"]

                    # PROCESS MESSAGE_BODY
                    f.write(message_body)
                    # print(message_body)

                    del_list.append({'ReceiptHandle': message["ReceiptHandle"],
                                     'Id': message["MessageId"]})

                if len(del_list) > 0:
                    client.delete_message_batch(QueueUrl=crawl_queue, Entries=del_list)

        tdata = TransferData(tc, "5ecf6444-affc-11e9-98d4-0a63aa6b37da", #TODO: Add source endpoint
                             globus_eid,
                             label=f"{crawl_id}")
        tdata.add_item(os.path.abspath(file_name), os.path.join(source_destination, file_name))

        tc.endpoint_autoactivate("5ecf6444-affc-11e9-98d4-0a63aa6b37da") #TODO: Add source endpoint
        tc.endpoint_autoactivate(globus_eid)
        submit_result = tc.submit_transfer(tdata)

        while True:
            result = tc.get_task(submit_result['task_id'])
            if result.data["status"] == "SUCCEEDED":
                break
            elif result.data["status"] == "FAILED":
                raise RuntimeError # TODO: Change this
            else:
                time.sleep(0.5)

    except Exception as e:
        print(e)
        abort(400, "Failed to validate")
    finally:
        os.remove(file_name)

    return "[200] Submitted"


if __name__ == "__main__":

    application.run(debug=True)







