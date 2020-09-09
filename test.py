import requests

if __name__ == "__main__":
    response = requests.post("http://127.0.0.1:5000/validate",
                  json={"crawl_id": "123test",
                        "globus_eid": "5ecf6444-affc-11e9-98d4-0a63aa6b37da",
                        "transfer_token": "AgMVzKJ7wlE7D8Kb2lVNnwba9eKvdD6P5MVr2nklM1w5Kb4nWjFOCnNnjXNBx7Bw302px5dQJ70voVivDkn7xUOKq0",
                        "source_destination": "/Users/ryan/Downloads",
                        "dataset_info": []})

    print(response)

