#Hashes the video and the remaining modules of the normal flow pipeline
#Produces a file with the resulting hash values
import hashlib
import pandas as pd
import time
import pickle
import json
from kafka import KafkaProducer
from config import VIDEO_PATH, BLOCK_SIZE, KAFKA_SERVER
from prometheus_client import start_http_server, Summary, Counter, Histogram
import random

REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')
COUNTER = Counter('module_throughput', 'Frames processed by the module')
HISTOGRAM = Histogram('hist_latency', 'Histogram for module latency', buckets=[0.1, 0.5, 1.0, 1.5, 1.9])

def hashing(filename):
    hash_value = hashlib.sha256()
    with open(filename, 'rb') as f:
        fb = f.read(BLOCK_SIZE)
        while len(fb) > 0:
            hash_value.update(fb)
            fb = f.read(BLOCK_SIZE)
    return hash_value.hexdigest()

def main():
    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: pickle.dumps(v), max_request_size=5000000, compression_type='gzip', )
            #producer_spark = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            print("Connected to Kafka broker")
            break
        except:
            time.sleep(3)
            pass

    start = 0
    while (start == 0):
        answer = input("Can I start?")
        if (answer == "y"):
            start += 1
        else:
            COUNTER.inc()
            tm = random.uniform(0.1, 1.9)
            time.sleep(tm)
            print(tm)
            REQUEST_TIME.observe(tm)
            HISTOGRAM.observe(tm)
    
    video_hash = hashing(VIDEO_PATH)
    module1_hash = hashing('module1.py')
    module2_hash = hashing('module2.py')
    module3_hash = hashing('module3.py')
    test = 1

    columns = ['video', 'module1', 'module2', 'module3']
    df = pd.DataFrame(columns=columns)
    df.set_index(['video'], inplace=True)

    df.loc[(video_hash), 'module1'] = module1_hash
    df.loc[(video_hash), 'module2'] = module2_hash
    df.loc[(video_hash), 'module3'] = module3_hash

    data = {
        "hash": video_hash
        }
    
    data2 = {
        "hash": "hash",
        "module1": "module1_hash",
        "module2": "module2_hash",
        "module3": "module3_hash",
        "test": test
    }
    
    # send frame and detections
    producer.send("m0_m1", data)
    #producer_spark.send("m0_spark", data2)
    print("Hash {}".format(video_hash))

    producer.flush()

    df.to_csv("output/hashing.csv")

    return 'Done'

if __name__ == "__main__":
    start_http_server(8000)
    main()