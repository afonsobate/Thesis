#Filters to focus only on the wanted person
import cv2
from ultralytics import YOLO
import supervision as sv
from kafka import KafkaConsumer, KafkaProducer
import pickle
from config import VIDEO_PATH, MODEL, KAFKA_SERVER
import numpy as np
import time
import csv
import pandas as pd

def filter_detections(detections, input):
    if (np.where(detections.tracker_id == np.int64(input))[0]).size > 0:
        index = np.where(detections.tracker_id == np.int64(input))[0][0]
        detections.tracker_id = np.array([detections.tracker_id[index]])
        detections.xyxy = np.array([detections.xyxy[index]])
        detections.confidence = np.array([detections.confidence[index]])
        detections.class_id = np.array([detections.class_id[index]])
    else:
        detections.tracker_id = np.array([])
    return detections

#NOT BEING USED
def apply_state_store(detections, input, i):
    x1 = 0
    x2 = 0
    y1 = 0
    y2 = 0
    with open('provenance/state_store/{}.csv'.format(i)) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            if (row[0] == str(input)):
                x1 = row[3]
                y1 = row[4]
                x2 = row[5]
                y2 = row[6]
    for j in range(detections.tracker_id.size):
        if ((abs(np.float32(x1) - detections.xyxy[j][0]) < 2) and (abs(np.float32(y1) - detections.xyxy[j][1]) < 2) and (abs(np.float32(x2) - detections.xyxy[j][2]) < 2) and (abs(np.float32(y2) - detections.xyxy[j][3]) < 2)):
            return detections.tracker_id[j]    
    return 0

def get_pos(detections, input):
    x1 = 0
    x2 = 0
    y1 = 0
    y2 = 0
    with open('output/output_store.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            if (row[0] == str(input)):
                x1 = row[2]
                y1 = row[3]
                x2 = row[4]
                y2 = row[5]
    for j in range(detections.tracker_id.size):
        if ((abs(np.float32(x1) - detections.xyxy[j][0]) < 2) and (abs(np.float32(y1) - detections.xyxy[j][1]) < 2) and (abs(np.float32(x2) - detections.xyxy[j][2]) < 2) and (abs(np.float32(y2) - detections.xyxy[j][3]) < 2)):
            return detections.tracker_id[j]    
    return 0

def main():

    while True:
        try:
            consumer = KafkaConsumer('r0_r1', value_deserializer=lambda v: pickle.loads(v), bootstrap_servers=[KAFKA_SERVER], group_id='module1', auto_offset_reset='earliest',)
            producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: pickle.dumps(v), max_request_size=5000000, compression_type='gzip', )
            print("Connected to Kafka broker")
            break
        except:
            time.sleep(3)
            pass

    for msg in consumer:
        input1 = int(msg.value['input1'])
        alert = int(msg.value['alert'])
        entry_frame = int(msg.value['entry_frame'])
        exit_frame = int(msg.value['exit_frame'])
        error = int(msg.value['error'])
        break

    if (error == 1):
        data = {"error": error}
        producer.send("r1_r2", data)
        producer.flush()
        consumer.commit()
        return 'Error'
    
    else:
        cap = cv2.VideoCapture(VIDEO_PATH)
        
        i = entry_frame//100

        if (i == 0):
            frame_number = 1
            model = YOLO(MODEL)
        else:
            frame_number = i * 100
            model = YOLO('output/state_store/{}.torchscript'.format(frame_number-1))
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number-1)

        while True:
            ret, frame = cap.read()
            if not ret:
                break

            results = model.track(frame, persist=True, classes = [0])
            result = results[0]

            detections = sv.Detections.from_yolov8(result)

            if result.boxes.id is not None:
                detections.tracker_id = result.boxes.id.cpu().numpy().astype(int)
                #if (frame_number == entry_frame):
                    #track = get_pos(detections, input1)
                detections = filter_detections(detections, input1)

            if detections.tracker_id.size != 0:
                tracked = 1
            else:
                tracked = 0

            frame = result.orig_img

            frame_encoded = cv2.imencode('.jpg', frame)[1]

            data = {
                "frame": frame_encoded,
                "detections": detections,
                "frame_number": frame_number,
                "person_id": input1,
                "alert": alert,
                "error": error,
                "tracked": tracked
            }

            # send frame and detections
            producer.send("r1_r2", data)

            frame_number += 1
        
        data = {
            "person_id": 0,
            "error": error
        }

        producer.send("r1_r2", data)

        producer.flush()
        consumer.commit()

        return 'Done'

if __name__ == "__main__":
    main()