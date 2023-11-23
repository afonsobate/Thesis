#Draws the zones and saves every frame
#produces a video with the tracking of the person and a file with every coordinates of that specific person from the frame its detected until the last one
import cv2
import supervision as sv
import numpy as np
import os
import pandas as pd
from kafka import KafkaConsumer
import pickle
import time
import shutil

from config import VIDEO_RESOLUTION_WH, VIDEO_FPS, KAFKA_SERVER, SUMMARY_P2, HISTOGRAM_P2, COUNTER_P2
from prometheus_client import Summary, Histogram, start_http_server

LATENCY_SUMMARY = Summary('module_latency_p2', 'Latency of frames in the module')
HISTOGRAM = Histogram('hist_latency_p2', 'Histogram for module latency', buckets=[0.005, 0.01, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50])


class Zone:
    def __init__(self, zone_id, polygon, frame_resolution_wh, color=sv.Color.white()):
        self.zone_id = zone_id
        self.polygon = polygon
        self.polygon_zone = sv.PolygonZone(polygon=polygon, frame_resolution_wh=frame_resolution_wh)
        self.polygon_zone_annotator = sv.PolygonZoneAnnotator(zone = self.polygon_zone, color=color, thickness=2)
        self.frame_resolution_wh = frame_resolution_wh
        self.tracker_ids_in_zone_prev = np.array([]) # keep track of the tracker_ids in the zones in the previous frame

    def process(self, detections, df, frame, frame_number):
        # zone_mask is an array of bools, where True means the detection is in the zone
        zone_mask = self.polygon_zone.trigger(detections=detections)
        frame = self.polygon_zone_annotator.annotate(scene=frame)

        # gets the tracker_ids of the detections in the zone
        tracker_ids_in_zone = detections.tracker_id[zone_mask]

        record_frame = False

        # new people in zones
        new_tracker_ids_in_zone = np.setdiff1d(tracker_ids_in_zone, self.tracker_ids_in_zone_prev)
        for tracker_id in new_tracker_ids_in_zone:
            if (tracker_id, self.zone_id) not in df.index: # if the was never seen in the zone. Don't overwrite the entry_frame if they re-enter the zone
                record_frame = True
                df.loc[(tracker_id, self.zone_id), 'entry_frame'] = frame_number

        # people leaving zones
        tracker_ids_not_in_zone = np.setdiff1d(self.tracker_ids_in_zone_prev, tracker_ids_in_zone)
        for tracker_id in tracker_ids_not_in_zone:
            record_frame = True
            df.loc[(tracker_id, self.zone_id), 'exit_frame'] = frame_number

        self.tracker_ids_in_zone_prev = tracker_ids_in_zone

        return frame, record_frame

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
            consumer = KafkaConsumer('p2_p3', value_deserializer=lambda v: pickle.loads(v), bootstrap_servers=[KAFKA_SERVER], group_id='provenance3', auto_offset_reset='earliest',)
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

    video_out = cv2.VideoWriter('provenance/alert1/prov_output.mp4', 
                         cv2.VideoWriter_fourcc(*'mp4v'),
                         VIDEO_FPS, VIDEO_RESOLUTION_WH)
    
    # pandas dataframe to store the data
    columns = ['person_id', 'frame', 'zone_id', 'x1', 'y1', 'x2', 'y2']
    df = pd.DataFrame(columns=columns)
    df.set_index(['person_id', 'frame'], inplace=True)

    frame_count = 1
    for msg in consumer:
        if (int(msg.value['error']) == 1):
            exit()

        if (int(msg.value['person']) == 0):
            false_positive = int(msg.value['false_positive'])
            cv2.destroyAllWindows()
            video_out.release()

            video_out = cv2.VideoWriter('provenance/alert{}/prov_output.mp4'.format(alert_num+1), 
                         cv2.VideoWriter_fourcc(*'mp4v'),
                         VIDEO_FPS, VIDEO_RESOLUTION_WH)

            df.to_csv("provenance/alert{}/output.csv".format(alert_num))
            df = pd.DataFrame(columns=columns)
            df.set_index(['person_id', 'frame'], inplace=True)

            frame_count = 1

            if false_positive == 1:
                shutil.rmtree('provenance/alert{}'.format(alert_num))

            continue

        if (int(msg.value['person']) == -1):
            break

        #print("Received frame:", frame_count)
        #Testing
        start_time = time.time()

        frame = cv2.imdecode(msg.value['frame'], cv2.IMREAD_COLOR)
        detections = msg.value["detections"]
        frame_number = int(msg.value['frame_number'])
        alert_num = int(msg.value['alert_num'])
        latency_start = float(msg.value['latency_start'])

        record_frame = False # used to determine if frame should be saved -> if a person enters or leaves the zone

        frame, zone1_record_frame = zone1.process(detections=detections, df=df, frame=frame, frame_number=frame_number)
        frame, zone2_record_frame = zone2.process(detections=detections, df=df, frame=frame, frame_number=frame_number)
        record_frame = zone1_record_frame or zone2_record_frame

        if record_frame:
            if not os.path.exists('provenance/alert{}/frames'.format(alert_num)):
                os.makedirs('provenance/alert{}/frames'.format(alert_num))

            cv2.imwrite(f"provenance/alert{alert_num}/frames/{frame_number}.jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 70])

        #cv2.imshow("yolov8", frame)
        video_out.write(frame)

        frame_count += 1

        #Testing
        end_time = time.time()
        latency = end_time - start_time
        latency_pip = end_time - latency_start
        LATENCY_SUMMARY.observe(latency)
        HISTOGRAM.observe(latency)
        SUMMARY_P2.observe(latency_pip)
        HISTOGRAM_P2.observe(latency_pip)

    consumer.commit()
    cv2.destroyAllWindows()
    video_out.release()
    #shutil.rmtree('provenance/prov_output{}.mp4'.format(alert_num+1))

    df.to_csv('provenance/alert{}/output.csv'.format(alert_num))

if __name__ == "__main__":
    start_http_server(8007)
    main()