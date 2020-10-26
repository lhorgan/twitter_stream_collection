import requests
import json
import time
from datetime import datetime
from threading import Thread
import queue
import random
import os
import sys
import logging
import traceback

consumer_key = os.environ.get("twitter_consumer_key") 
consumer_secret = os.environ.get("twitter_consumer_secret")
records_per_file = 5000  # Replace this with the number of tweets you want to store per file
file_path = sys.argv[1] # make sure to include a trailing /

print(consumer_key)
print(consumer_secret)

count = 0
file_object = None
file_name = None
dead_partitions = queue.Queue()
tweets_to_write = queue.Queue()
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
def save_data():
    global file_object, file_name, current_day, tweets_to_write, kill
    time_to_sleep = 0.05
    
    current_day = datetime.date(datetime.utcnow())
    day = current_day
    if file_object is None:
        file_name = day.strftime("%m-%d-%Y")     
        file_object = open(f'{file_path}covid19_{file_name}.csv', 'a')

    while not kill:
        day = datetime.date(datetime.utcnow())
        if day != current_day:
            print("It's a new day!")
            current_day =  day
            file_object.close()
            file_name = day.strftime("%m-%d-%Y")
            file_object = open(f'{file_path}covid19_{file_name}.csv', 'a')
        
        item = tweets_to_write.get()
        if item is not None:
            file_object.write("{}\n".format(json.dumps(item)))
        
        qsize = tweets_to_write.qsize()
        if tweets_to_write.qsize() > 2:
            time_to_sleep -= 0.001
        else:
            time_to_sleep += 0.001
        
        #print("Sleeping for %0.5f, queue size is %i" % (time_to_sleep, qsize))
        time.sleep(time_to_sleep)


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
                data = json.loads(response_line)
                try:
                    if data['lang'] == 'en' :
                        tweets_to_write.put(data)
                except KeyError:
                    continue
            if kill == True:
                return # end this thread
    except (requests.exceptions.ConnectionError, OSError) as e:
        print(e)
        print("Error in thread for parition %i" % partition)
        dead_partitions.put(partition)
        return # explicitly end this thread
    except Exception as e:
        print(e)
        print("Unexpected error in thread for parition %i" % partition)
        logging.error(traceback.format_exc())
        # Logs the error appropriately. 
        dead_partitions.put(partition)
        return

def main():
    global kill, dead_partitions

    timeout = 0
    for partition in range(1, 5):
        Thread(target=stream_connect, args=(partition,)).start()
    
    Thread(target=save_data, args=()).start()
    
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