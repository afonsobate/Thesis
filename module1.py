#Detects people in the video, divides the original video into smaller videos
#Produces a file where it registers in what frame every person was first detected and last detected, as well as the coordenates of their positions in those moments
import cv2
from ultralytics import YOLO
import supervision as sv
from kafka import KafkaProducer, KafkaConsumer
import pickle
import sympy
import time
import numpy as np
import pandas as pd
import os
import jsonpickle
import shutil
import json
from config import VIDEO_PATH, MODEL, KAFKA_SERVER
from prometheus_client import Summary, Histogram, start_http_server

LATENCY_SUMMARY = Summary('module_latency_p1', 'Latency of frames in the module')
HISTOGRAM = Histogram('hist_latency_p1', 'Histogram for module latency', buckets=[0.005, 0.01, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50])

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
            consumer = KafkaConsumer('m0_m1', value_deserializer=lambda v: pickle.loads(v), bootstrap_servers=[KAFKA_SERVER], group_id='module1', auto_offset_reset='earliest',)
            producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: pickle.dumps(v), max_request_size=5000000, compression_type='gzip', )
            producer_spark = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            print("Connected to Kafka broker")
            break
        except:
            time.sleep(3)
            pass

    for msg in consumer:
        hash = msg.value['hash']
        break

    model = YOLO(MODEL)

    frame_count = 1
    
    if not os.path.exists("output/state_store"):
        os.makedirs("output/state_store")

    columns = ['person_id', 'entry_frame', 'entry_x1', 'entry_y1', 'entry_x2', 'entry_y2', 'exit_frame', 'exit_x1', 'exit_y1', 'exit_x2', 'exit_y2']
    df = pd.DataFrame(columns=columns)
    df.set_index(['person_id'], inplace=True)

    alerts = {}
    num_alerts = 0
    counter = 0
    while (counter < 1000):
        for result in model.track(VIDEO_PATH, show=False, stream=True, agnostic_nms=False, classes=[0],):
            
            #Testing
            start_time = time.time()

            detections = sv.Detections.from_yolov8(result)

            if result.boxes.id is not None:
                detections.tracker_id = result.boxes.id.cpu().numpy().astype(int)
                produceList(detections, frame_count, df=df)

            if (frame_count%100 == 99):
                exported_model = model.export()
                shutil.copyfile('{}'.format(exported_model), 'output/state_store/{}.torchscript'.format(frame_count))
                with open('output/state_store/predictor{}.pkl'.format(frame_count), 'wb') as f:
                    pickle.dump(model.predictor.__getattribute__('trackers'), f)

            if (frame_count > 1):
                alert = findMissing(prev_det, detections.tracker_id.tolist())
                if alert:
                    alerts[frame_count] = alert
                    for i in alert:
                        num_alerts += 1
                        data_prov = {
                            "person": i,
                            "entry_frame": df.loc[i, 'entry_frame'],
                            "exit_frame": frame_count,
                            "entry_x1": df.loc[i, 'entry_x1'],
                            "entry_y1": df.loc[i, 'entry_y1'],
                            "entry_x2": df.loc[i, 'entry_x2'],
                            "entry_y2": df.loc[i, 'entry_y2'],
                            "alert_num": num_alerts
                        }
                        producer.send("m_p", data_prov)
                        data_spark = {
                            "person": i,
                            "zone": 0,
                            "entry_frame": df.loc[i, 'entry_frame'],
                            "exit_frame": frame_count,
                            "latency_start": time.time() - start_time
                        }
                        producer_spark.send("m1_spark", data_spark)

            prev_det = detections.tracker_id.tolist()

            frame = result.orig_img

            frame_encoded = cv2.imencode('.jpg', frame)[1]

            data = {
                "frame": frame_encoded,
                "detections": detections,
                "frame_count": frame_count,
                "hash": hash,
                "latency_start": start_time
            }

            frame_count += 1

            # send frame and detections
            producer.send("m1_m2", data)

            #Testing
            end_time = time.time()
            latency = end_time - start_time
            LATENCY_SUMMARY.observe(latency)
            HISTOGRAM.observe(latency)
        counter += 1

    
    df.to_csv("output/output_store.csv")

    serialized = jsonpickle.encode(alerts)

    with open('output/state_store/alerts.json', 'w') as f:
        json.dump(serialized, f)

    frame_count = 0

    data = {
            "frame": frame_encoded,
            "detections": detections,
            "frame_count": frame_count,
            "hash": hash
    }

    producer.send("m1_m2", data)

    data = {
        "person": frame_count
    }
    
    producer.send("m_p", data)

    producer.flush()
    consumer.commit()  

if __name__ == "__main__":
    start_http_server(8001)
    main()