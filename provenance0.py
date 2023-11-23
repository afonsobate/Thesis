#Receives the id of the person to track and gathers its entry frame and exit frame to potientialy combine video fragments
#It also checks if the hashing of the original video and modules is the same as at the time of producing the results 
import hashlib
import csv
import time
import pickle
import pandas as pd
import os
from kafka import KafkaProducer, KafkaConsumer
from config import VIDEO_PATH, KAFKA_SERVER, BLOCK_SIZE, COUNTER_P2
from prometheus_client import start_http_server

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
            #consumer = KafkaConsumer('m_p', value_deserializer=lambda v: pickle.loads(v), bootstrap_servers=[KAFKA_SERVER], group_id='provenance0', auto_offset_reset='earliest',)
            consumer = KafkaConsumer('A_B', value_deserializer=lambda v: pickle.loads(v), bootstrap_servers=[KAFKA_SERVER], group_id='provenance0', auto_offset_reset='earliest',)
            producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: pickle.dumps(v), max_request_size=5000000, compression_type='gzip', )
            print("Connected to Kafka broker")
            break
        except:
            print("Not connecting")
            time.sleep(3)
            pass
    
    #for msg in consumer_test:
        #con = msg.value['con']
        #print(con)
        #if con == "true":
            #break

    for msg in consumer:
        person = int(msg.value['person'])
        error = 0

        if person == 0:
            data = {
                "person": person,
                "error": error
            }

            producer.send("p0_p1", data)
            break

        entry_frame = int(msg.value['entry_frame'])
        exit_frame = int(msg.value['exit_frame'])
        entry_x1 = float(msg.value['entry_x1'])
        entry_y1 = float(msg.value['entry_y1'])
        entry_x2 = float(msg.value['entry_x2'])
        entry_y2 = float(msg.value['entry_y2'])
        alert_num = int(msg.value['alert_num'])

        if not os.path.exists("provenance"):
            os.makedirs("provenance")

        if not os.path.exists('provenance/alert{}'.format(alert_num)):
            os.makedirs('provenance/alert{}'.format(alert_num))

        #with open('output/hashing.csv') as csv_file:
            #csv_reader = csv.reader(csv_file, delimiter=',')
            #for row in csv_reader:
                #if (row[0] == "video"):
                    #continue
                #if (row[0] != hashing(VIDEO_PATH) or row[1] != hashing('module1.py') or row[2] != hashing('module2.py') or row[3] != hashing('module3.py')):
                    #print(row[0])
                    #print(hashing(VIDEO_PATH))
                    #error = 1
                    #print("Unexpected result when hashing")

        video_hash = hashing(VIDEO_PATH)
        module1_hash = hashing('provenance1.py')
        module2_hash = hashing('provenance2.py')
        module3_hash = hashing('provenance3.py')

        columns = ['video', 'provenance1', 'provenance2', 'provenance3']
        df = pd.DataFrame(columns=columns)
        df.set_index(['video'], inplace=True)

        df.loc[(video_hash), 'provenance1'] = module1_hash
        df.loc[(video_hash), 'provenance2'] = module2_hash
        df.loc[(video_hash), 'provenance3'] = module3_hash

        data = {
            "person": person,
            "entry_frame": entry_frame,
            "exit_frame": exit_frame,
            "error": error,
            "entry_x1": entry_x1,
            "entry_y1": entry_y1,
            "entry_x2": entry_x2,
            "entry_y2": entry_y2,
            "alert_num": alert_num
        }

        producer.send("p0_p1", data)

        df.to_csv('provenance/alert{}/hashing.csv'.format(alert_num))

        COUNTER_P2.inc()

    producer.flush()
    consumer.commit()

    return 'Done'

if __name__ == "__main__":
    start_http_server(8004)
    main()