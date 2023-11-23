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
import jsonpickle
import json
import os
from prometheus_client import Summary, Histogram, start_http_server

LATENCY_SUMMARY = Summary('module_latency_p2', 'Latency of frames in the module')
HISTOGRAM = Histogram('hist_latency_p2', 'Histogram for module latency', buckets=[0.005, 0.01, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50])

def leave_store(detections, input):
    if (np.where(detections.tracker_id == np.int64(input))[0]).size > 0:
        index = np.where(detections.tracker_id == np.int64(input))[0][0]
        x1 = float(detections.xyxy[index][0])
        y1 = float(detections.xyxy[index][1])
        x2 = float(detections.xyxy[index][2])
        y2 = float(detections.xyxy[index][3])
        if ((abs(x1 - 0.0) < 1.0) or (abs(y1 - 0.0) < 1.0) or (abs(x2 - 960.0) < 1) or ((abs(y2 - 540.0) < 1))):
            return True
    return False

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

def get_pos(detections, x1, y1, x2, y2):
    for j in range(detections.tracker_id.size):
        if ((abs(np.float32(x1) - detections.xyxy[j][0]) < 2) and (abs(np.float32(y1) - detections.xyxy[j][1]) < 2) and (abs(np.float32(x2) - detections.xyxy[j][2]) < 2) and (abs(np.float32(y2) - detections.xyxy[j][3]) < 2)):
            return detections.tracker_id[j]    
    return 0

def produceList(detections, frame, df):
    for i in detections.tracker_id:
        j = np.where(detections.tracker_id == i)
        if i not in df.index:
            df.loc[i, 'entry_frame'] = frame
            df.loc[i, 'entry_x1'] = (detections.xyxy[j][0])[0]
            df.loc[i, 'entry_y1'] = (detections.xyxy[j][0])[1]
            df.loc[i, 'entry_x2'] = (detections.xyxy[j][0])[2]
            df.loc[i, 'entry_y2'] = (detections.xyxy[j][0])[3]
        df.loc[i, 'exit_frame'] = frame
        df.loc[i, 'exit_x1'] = (detections.xyxy[j][0])[0]
        df.loc[i, 'exit_y1'] = (detections.xyxy[j][0])[1]
        df.loc[i, 'exit_x2'] = (detections.xyxy[j][0])[2]
        df.loc[i, 'exit_y2'] = (detections.xyxy[j][0])[3]

def findMissing(a, b):
    alert = []
    result = set(a).difference(set(b))
    for r in result:
        alert.append(r)
    return alert

def main():

    while True:
        try:
            consumer = KafkaConsumer('p0_p1', value_deserializer=lambda v: pickle.loads(v), bootstrap_servers=[KAFKA_SERVER], group_id='provenance1', auto_offset_reset='earliest',)
            producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: pickle.dumps(v), max_request_size=5000000, compression_type='gzip', )
            print("Connected to Kafka broker")
            number_of_alerts = 0
            break
        except:
            time.sleep(3)
            pass

    for msg in consumer:
        person = int(msg.value['person'])
        error = int(msg.value['error'])

        if (error == 1):
            data = {"error": error}
            producer.send("p1_p2", data)
            producer.flush()
            consumer.commit()
            return 'Error'
        
        elif person == 0:
            break

        else:
            entry_frame = int(msg.value['entry_frame'])
            exit_frame = int(msg.value['exit_frame'])
            entry_x1 = float(msg.value['entry_x1'])
            entry_y1 = float(msg.value['entry_y1'])
            entry_x2 = float(msg.value['entry_x2'])
            entry_y2 = float(msg.value['entry_y2'])
            alert_num = int(msg.value['alert_num'])

            number_of_alerts += 1
            if (alert_num == number_of_alerts):
                print('Received alert number: {}'.format(number_of_alerts))
            else:
                print("Something went wrong")

            cap = cv2.VideoCapture(VIDEO_PATH)
            
            i = entry_frame//100

            if (i == 0):
                frame_number = 1
                model = YOLO(MODEL)
            else:
                frame_number = i * 100
                model = YOLO('output/state_store/{}.torchscript'.format(frame_number-1))
                cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number-1)

            columns = ['person_id', 'entry_frame', 'entry_x1', 'entry_y1', 'entry_x2', 'entry_y2', 'exit_frame', 'exit_x1', 'exit_y1', 'exit_x2', 'exit_y2']
            df = pd.DataFrame(columns=columns)
            df.set_index(['person_id'], inplace=True)

            alerts = {}
            start_frame = frame_number
            track = 0

            while True:
                #Testing
                start_time = time.time()

                ret, frame = cap.read()
                if not ret:
                    break

                results = model.track(frame, persist=True, classes = [0])
                result = results[0]

                detections = sv.Detections.from_yolov8(result)

                if result.boxes.id is not None:
                    detections.tracker_id = result.boxes.id.cpu().numpy().astype(int)
                    produceList(detections, frame_number, df=df)
                    if (frame_number == entry_frame):
                        track = get_pos(detections, entry_x1, entry_y1, entry_x2, entry_y2)

                if (track > 0 and frame_number > start_frame):
                    alert = findMissing(ids_prev_det, detections.tracker_id.tolist())
                    if (track in alert) and (frame_number == exit_frame) and leave_store(prev_det, track):
                        alerts[frame_number] = track

                prev_det = detections
                ids_prev_det = detections.tracker_id.tolist()

                frame = result.orig_img

                frame_encoded = cv2.imencode('.jpg', frame)[1]

                data = {
                    "person": person,
                    "frame": frame_encoded,
                    "detections": detections,
                    "frame_number": frame_number,
                    "error": error,
                    "alert_num": alert_num,
                    "latency_start": start_time
                }

                # send frame and detections
                producer.send("p1_p2", data)

                frame_number += 1

                #Testing
                end_time = time.time()
                latency = end_time - start_time
                LATENCY_SUMMARY.observe(latency)
                HISTOGRAM.observe(latency)

            person = 0
            false_positive = 1
            
            if len(alerts) > 0:
                df.to_csv('provenance/alert{}/output_store.csv'.format(alert_num))

                serialized = jsonpickle.encode(alerts)

                if not os.path.exists("provenance/alerts"):
                    os.makedirs("provenance/alerts")

                with open('provenance/alerts/alert{}.json'.format(alert_num), 'w') as f:
                    json.dump(serialized, f)

                false_positive = 0

            data = {
                "person": person,
                "false_positive": false_positive,
                "error": error
            }

            producer.send("p1_p2", data)

    person = -1

    data = {
            "person": person,
            "error": error
        }
    
    producer.send("p1_p2", data)

    producer.flush()
    consumer.commit()

    return 'Done'

if __name__ == "__main__":
    start_http_server(8005)
    main()