#Annotates each frame by putting a box with the id around the desired person
import cv2
import supervision as sv
from kafka import KafkaConsumer, KafkaProducer
import pickle
import time
from config import KAFKA_SERVER
from prometheus_client import Summary, Histogram, start_http_server

LATENCY_SUMMARY = Summary('module_latency_p2', 'Latency of frames in the module')
HISTOGRAM = Histogram('hist_latency_p2', 'Histogram for module latency', buckets=[0.005, 0.01, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50])

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
            consumer = KafkaConsumer('p1_p2', value_deserializer=lambda v: pickle.loads(v), bootstrap_servers=[KAFKA_SERVER], group_id='provenance2', auto_offset_reset='earliest',)
            producer = KafkaProducer(bootstrap_servers=[KAFKA_SERVER], value_serializer=lambda v: pickle.dumps(v), max_request_size=100000000)
            print("Connected to Kafka broker")
            break
        except:
            time.sleep(3)
            pass

    person_annotator = PersonBoundingBoxAnnotator()

    frame_count = 1
    for msg in consumer:
        error = int(msg.value['error'])
        person = int(msg.value['person'])
        
        if (error == 1):
            data = {"error": msg.value['error']}
            producer.send("p2_p3", data)
            break

        elif (person == 0):
            false_positive = int(msg.value['false_positive'])
            data = {
                "person": person,
                "false_positive": false_positive,
                "error": error
            }
            producer.send('p2_p3', data)

            person_annotator = PersonBoundingBoxAnnotator()

            frame_count = 1

            continue

        elif (person == -1):
            data = {
                "person": person,
                "error": error
            }
            producer.send('p2_p3', data)

            break

        else:
            #Testing
            start_time = time.time()

            frame = cv2.imdecode(msg.value['frame'], cv2.IMREAD_COLOR)
            detections = msg.value['detections']
            frame_number = int(msg.value['frame_number'])
            latency_start = float(msg.value['latency_start'])

            #print("Received frame:", frame_count)

            alert_num = int(msg.value['alert_num'])
            
            frame = person_annotator.annotate(frame=frame, detections=detections)

            frame_encoded = cv2.imencode('.jpg', frame)[1]

            # outputs
            # - frame
            # - detections
            data = {
                "person": person,
                "frame": frame_encoded,
                "detections": detections,
                "frame_number": frame_number,
                "error": error,
                "alert_num": alert_num,
                "latency_start": latency_start
            }
            
            #send json
            producer.send('p2_p3', data)
            #print("Sent frame:", frame_count)

            frame_count += 1

            #Testing
            end_time = time.time()
            latency = end_time - start_time
            LATENCY_SUMMARY.observe(latency)
            HISTOGRAM.observe(latency)

    producer.flush()
    consumer.commit()

if __name__ == "__main__":
    start_http_server(8006)
    main()