#Receives the id of the person to track and gathers its entry frame and exit frame to potientialy combine video fragments
#It also checks if the hashing of the original video and modules is the same as at the time of producing the results
import hashlib
import csv
import time
import pickle
from kafka import KafkaProducer
from config import VIDEO_PATH, KAFKA_SERVER, BLOCK_SIZE

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
            print("Connected to Kafka broker")
            break
        except:
            time.sleep(3)
            pass

    input1 = input("Enter person to track:")
    input2 = input("Enter alert number:")
    person = True
    error = 0
    entry_frame = 0
    exit_frame = 0

    with open('provenance/alert{}/output_store.csv'.format(input2)) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            if (row[0] == input1):
                person = False
                entry_frame = row[1]
                exit_frame = row[6]     
        if person:
            error = 1
            print("Person id doesnt exist")

    with open('provenance/alert{}/hashing.csv'.format(input2)) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            if (row[0] == "video"):
                continue
            if (row[0] != hashing(VIDEO_PATH) or row[1] != hashing('provenance1.py') or row[2] != hashing('provenance2.py') or row[3] != hashing('provenance3.py')):
                print(row[0])
                print(hashing(VIDEO_PATH))
                error = 1
                print("Unexpected result when hashing")

    data = {
        "input1": input1,
        "alert": input2,
        "entry_frame": entry_frame,
        "exit_frame": exit_frame,
        "error": error
    }

    producer.send("r0_r1", data)

    producer.flush()

if __name__ == "__main__":
    main()