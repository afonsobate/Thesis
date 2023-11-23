#Draws the zones in the video and detectes if people are inside a zone and when they first entered a zone and last left a zone, specifying which one
#Produces state store files, every relevant frame, videos with the annotations and drawings, and a file with entering and leaving of zones for each person
import cv2
import supervision as sv
import numpy as np
import os
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
import pickle
import time
import json

from config import VIDEO_RESOLUTION_WH, VIDEO_FPS, KAFKA_SERVER, SUMMARY_P1, HISTOGRAM_P1
from prometheus_client import Summary, Histogram, start_http_server

LATENCY_SUMMARY = Summary('module_latency_p1', 'Latency of frames in the module')
HISTOGRAM = Histogram('hist_latency_p1', 'Histogram for module latency', buckets=[0.005, 0.01, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50])

class Zone:
    def __init__(self, zone_id, polygon, frame_resolution_wh, color=sv.Color.white()):
        self.zone_id = zone_id
        self.polygon = polygon
        self.polygon_zone = sv.PolygonZone(polygon=polygon, frame_resolution_wh=frame_resolution_wh)
        self.polygon_zone_annotator = sv.PolygonZoneAnnotator(zone = self.polygon_zone, color=color, thickness=2)
        self.frame_resolution_wh = frame_resolution_wh
        self.tracker_ids_in_zone_prev = np.array([]) # keep track of the tracker_ids in the zones in the previous frame

    def process(self, detections, df, frame, frame_count):
        # zone_mask is an array of bools, where True means the detection is in the zone
        zone_mask = self.polygon_zone.trigger(detections=detections)
        frame = self.polygon_zone_annotator.annotate(scene=frame)
        data = []

        # gets the tracker_ids of the detections in the zone
        tracker_ids_in_zone = detections.tracker_id[zone_mask]

        record_frame = False
        send_data = False

        # new people in zones
        new_tracker_ids_in_zone = np.setdiff1d(tracker_ids_in_zone, self.tracker_ids_in_zone_prev)
        for tracker_id in new_tracker_ids_in_zone:
            if (tracker_id, self.zone_id) not in df.index: # if the was never seen in the zone. Don't overwrite the entry_frame if they re-enter the zone
                record_frame = True
                df.loc[(tracker_id, self.zone_id), 'entry_frame'] = frame_count

        # people leaving zones
        tracker_ids_not_in_zone = np.setdiff1d(self.tracker_ids_in_zone_prev, tracker_ids_in_zone)
        for tracker_id in tracker_ids_not_in_zone:
            record_frame = True
            df.loc[(tracker_id, self.zone_id), 'exit_frame'] = frame_count
            data_spark = {
                "person": int(tracker_id),
                "zone": int(self.zone_id),
                "entry_frame": int(df.loc[(tracker_id, self.zone_id), 'entry_frame']),
                "exit_frame": frame_count
            }
            data.append(data_spark)
            send_data = True

        self.tracker_ids_in_zone_prev = tracker_ids_in_zone

        return frame, record_frame, data, send_data

class PersonBoundingBoxAnnotator:
    def __init__(self, thickness=2, text_thickness=1, text_scale=0.4):
        self.box_annotator = sv.BoxAnnotator(thickness=thickness, text_thickness=text_thickness, text_scale=text_scale)

    def annotate(self, frame, detections):
        labels = [
            f"#{tracker_id}"
            for _, confidence, class_id, tracker_id
            in detections
        ]

        return self.box_annotator.annotate(
            scene=frame, 
            detections=detections,
            labels=labels
        )

def main():
    while True:
        try:
            consumer = KafkaConsumer('m2_m3', value_deserializer=lambda v: pickle.loads(v), bootstrap_servers=[KAFKA_SERVER], group_id='module3', auto_offset_reset='earliest',)
            producer_spark = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            print("Connected to Kafka broker")
            break
        except:
            time.sleep(3)
            pass


    # use https://roboflow.github.io/polygonzone/
    zone1_polygon = np.array([[0, 320], [80, 260], [480, 260], [400, 535], [0, 535]])
    zone2_polygon = np.array([[540, 200], [800, 200], [890, 360], [500, 360]])

    zone1 = Zone(zone_id=1, polygon=zone1_polygon, frame_resolution_wh=VIDEO_RESOLUTION_WH)
    zone2 = Zone(zone_id=2, polygon=zone2_polygon, frame_resolution_wh=VIDEO_RESOLUTION_WH, color=sv.Color.green())


    if not os.path.exists("output"):
        os.makedirs("output")

    if not os.path.exists("output/frames"):
        os.makedirs("output/frames")

    if not os.path.exists("output/videos"):
        os.makedirs("output/videos")

    video_out = cv2.VideoWriter('output/videos/output.mp4', 
                         cv2.VideoWriter_fourcc(*'mp4v'),
                         VIDEO_FPS, VIDEO_RESOLUTION_WH)
    
    # pandas dataframe to store the data
    columns = ['person_id', 'zone_id', 'entry_frame', 'exit_frame']
    df = pd.DataFrame(columns=columns)
    df.set_index(['person_id', 'zone_id'], inplace=True)

    for msg in consumer:
        frame = cv2.imdecode(msg.value['frame'], cv2.IMREAD_COLOR)
        detections = msg.value["detections"]
        frame_count = msg.value["frame_count"]
        hash = msg.value['hash']
        latency_start = msg.value['latency_start']

        if (frame_count == 0):
            break

        #Testing
        start_time = time.time()

        #print("Received frame:", frame_count)

        record_frame = False # used to determine if frame should be saved -> if a person enters or leaves the zone
    
        frame, zone1_record_frame, data1, send_data1 = zone1.process(detections=detections, df=df, frame=frame, frame_count=frame_count)
        frame, zone2_record_frame, data2, send_data2 = zone2.process(detections=detections, df=df, frame=frame, frame_count=frame_count)
        record_frame = zone1_record_frame or zone2_record_frame

        if record_frame:
            cv2.imwrite(f"output/frames/{frame_count}.jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 70])
            if send_data1:
                for i in data1:
                    i.update({"latency": time.time() - latency_start})
                    producer_spark.send("m3_spark", i)
            if send_data2:
                for i in data2:
                    i.update({"latency": time.time() - latency_start})
                    producer_spark.send("m3_spark", i)
                

        #cv2.imshow("yolov8", frame)
        video_out.write(frame)
        
        #Testing
        end_time = time.time()
        latency = end_time - start_time
        latency_pip = end_time - latency_start
        LATENCY_SUMMARY.observe(latency)
        HISTOGRAM.observe(latency)
        SUMMARY_P1.observe(latency_pip)
        HISTOGRAM_P1.observe(latency_pip)

    consumer.commit()
    cv2.destroyAllWindows()
    video_out.release()

    df.to_csv("output/output_zone.csv")

if __name__ == "__main__":
    start_http_server(8003)
    main()