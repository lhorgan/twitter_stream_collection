import requests
import json
import time
from datetime import datetime
from threading import Thread
import queue
import random
import os
import sys

consumer_key = os.environ.get("twitter_consumer_key") 
consumer_secret = os.environ.get("twitter_consumer_secret")
records_per_file = 5000  # Replace this with the number of tweets you want to store per file
file_path = sys.argv[1] # make sure to include a trailing /

count = 0
file_object = None
file_name = None
dead_partitions = queue.Queue()
kill = False


def get_bearer_token(key, secret):
    response = requests.post(
        "https://api.twitter.com/oauth2/token",
        auth=(key, secret),
        data={'grant_type': 'client_credentials'},
        headers={"User-Agent": "TwitterDevCovid19StreamQuickStartPython"})

    if response.status_code != 200:
        raise Exception(f"Cannot get a Bearer token (HTTP %d): %s" % (response.status_code, response.text))

    body = response.json()
    return body['access_token']


# Helper method that saves the tweets to a file at the specified path
def save_data(item):
    global file_object, count, file_name
    if file_object is None:
        file_name = int(datetime.utcnow().timestamp() * 1e3)
        count += 1
        file_object = open(f'{file_path}covid19-{file_name}.csv', 'a')
        file_object.write("{}\n".format(item))
        return
    if count == records_per_file:
        file_object.close()
        count = 1
        file_name = int(datetime.utcnow().timestamp() * 1e3)
        file_object = open(f'{file_path}covid19-{file_name}.csv', 'a')
        file_object.write("{}\n".format(item))
    else:
        count += 1
        file_object.write("{}\n".format(item))


def stream_connect(partition):
    global kill, dead_partitions

    print("Creating stream for partition %i" % partition)

    try:
        response = requests.get("https://api.twitter.com/labs/1/tweets/stream/covid19?partition={}".format(partition),
                                headers={"User-Agent": "TwitterDevCovid19StreamLazerLab",
                                        "Authorization": "Bearer {}".format(
                                            get_bearer_token(consumer_key, consumer_secret))},
                                stream=True)
        for response_line in response.iter_lines():
            # if random.random() < 0.01:
            #     raise OSError(2, "Something went wrong in partition %i. This is totally not a drill." % partition, "simulated error")

            if response_line:
                save_data(json.loads(response_line))
            if kill == True:
                return # end this thread
    except (requests.exceptions.ConnectionError, OSError) as e:
        print(e)
        print("Error in thread for parition %i" % partition)
        dead_partitions.put(partition)
        return # explicitly end this thread

def main():
    global kill, dead_partitions

    timeout = 0
    for partition in range(1, 5):
        Thread(target=stream_connect, args=(partition,)).start()
    
    while not kill:
        try:
            partition = dead_partitions.get()
            if partition is not None:
                Thread(target=stream_connect, args=(partition,)).start()
            time.sleep(1000)
        except KeyboardInterrupt:
            kill = True

if __name__ == "__main__":
    main()