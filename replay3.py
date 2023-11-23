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


from config import VIDEO_RESOLUTION_WH, VIDEO_FPS, KAFKA_SERVER


class Zone:
    def __init__(self, zone_id, polygon, frame_resolution_wh, color=sv.Color.white()):
        self.zone_id = zone_id
        self.polygon = polygon
        self.polygon_zone = sv.PolygonZone(polygon=polygon, frame_resolution_wh=frame_resolution_wh)
        self.polygon_zone_annotator = sv.PolygonZoneAnnotator(zone = self.polygon_zone, color=color, thickness=2)
        self.frame_resolution_wh = frame_resolution_wh
        self.tracker_ids_in_zone_prev = np.array([]) # keep track of the tracker_ids in the zones in the previous frame

    def process(self, detections, df, frame, person_id, frame_number):
        # zone_mask is an array of bools, where True means the detection is in the zone
        zone_mask = self.polygon_zone.trigger(detections=detections)
        frame = self.polygon_zone_annotator.annotate(scene=frame)

        # gets the tracker_ids of the detections in the zone
        tracker_ids_in_zone = detections.tracker_id[zone_mask]

        record_frame = False

        if tracker_ids_in_zone.size > 0:
            record_frame = True
            for tracker_id in tracker_ids_in_zone:
                df.loc[(person_id, frame_number), 'zone_id'] = self.zone_id
                df.loc[(person_id, frame_number), 'x1'] = detections.xyxy[0][0]
                df.loc[(person_id, frame_number), 'y1'] = detections.xyxy[0][1]
                df.loc[(person_id, frame_number), 'x2'] = detections.xyxy[0][2]
                df.loc[(person_id, frame_number), 'y2'] = detections.xyxy[0][3]
        
        # new people in zones
        #new_tracker_ids_in_zone = np.setdiff1d(tracker_ids_in_zone, self.tracker_ids_in_zone_prev)
        #for tracker_id in new_tracker_ids_in_zone:
        #    if (tracker_id, self.zone_id) not in df.index: # if the was never seen in the zone. Don't overwrite the entry_frame if they re-enter the zone
        #        df.loc[(tracker_id, self.zone_id), 'entry_frame'] = frame_count
        #        df.loc[(tracker_id, self.zone_id), 'hash'] = hash

        # people leaving zones
        #tracker_ids_not_in_zone = np.setdiff1d(self.tracker_ids_in_zone_prev, tracker_ids_in_zone)
        #for tracker_id in tracker_ids_not_in_zone:
        #    df.loc[(tracker_id, self.zone_id), 'exit_frame'] = frame_count

        #self.tracker_ids_in_zone_prev = tracker_ids_in_zone

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
            consumer = KafkaConsumer('r2_r3', value_deserializer=lambda v: pickle.loads(v), bootstrap_servers=[KAFKA_SERVER], group_id='module3', auto_offset_reset='earliest',)
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


    if not os.path.exists("replay"):
        os.makedirs("replay")

    video_out = cv2.VideoWriter('replay/replay_output.mp4', 
                         cv2.VideoWriter_fourcc(*'mp4v'),
                         VIDEO_FPS, VIDEO_RESOLUTION_WH)
    
    # pandas dataframe to store the data
    columns = ['person_id', 'frame', 'zone_id', 'x1', 'y1', 'x2', 'y2']
    df = pd.DataFrame(columns=columns)
    df.set_index(['person_id', 'frame'], inplace=True)

    frame_count = 1
    person = 0
    for msg in consumer:
        if (int(msg.value['error']) == 1):
            exit()

        print("Received frame:", frame_count)
            
        person_id = int(msg.value['person_id'])
        if (person_id == 0):
            break
        else:
            person = person_id
        frame = cv2.imdecode(msg.value['frame'], cv2.IMREAD_COLOR)
        detections = msg.value["detections"]
        frame_number = int(msg.value['frame_number'])
        tracked = int(msg.value['tracked'])
        alert = int(msg.value['alert'])

        if tracked == 1:
            frame, zone1_record_frame = zone1.process(detections=detections, df=df, frame=frame, person_id = person_id, frame_number=frame_number)
            frame, zone2_record_frame = zone2.process(detections=detections, df=df, frame=frame, person_id = person_id, frame_number=frame_number)
            record_frame = zone1_record_frame or zone2_record_frame

            if not record_frame:
                df.loc[(person_id, frame_number), 'zone_id'] = 0
                df.loc[(person_id, frame_number), 'x1'] = detections.xyxy[0][0]
                df.loc[(person_id, frame_number), 'y1'] = detections.xyxy[0][1]
                df.loc[(person_id, frame_number), 'x2'] = detections.xyxy[0][2]
                df.loc[(person_id, frame_number), 'y2'] = detections.xyxy[0][3]

        if not os.path.exists("replay/alert{}/frames/person{}".format(alert, person_id)):
            os.makedirs("replay/alert{}/frames/person{}".format(alert, person_id))

        cv2.imwrite(f"replay/alert{alert}/frames/person{person_id}/{frame_number}.jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 70])

        #cv2.imshow("yolov8", frame)
        video_out.write(frame)

        frame_count += 1

    consumer.commit()
    cv2.destroyAllWindows()
    video_out.release()

    os.rename('replay/replay_output.mp4', 'replay/alert{}/replay_output{}.mp4'.format(alert, person))
    df.to_csv("replay/alert{}/output{}.csv".format(alert, person))

if __name__ == "__main__":
    main()