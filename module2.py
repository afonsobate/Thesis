#Annotates each frame, putting a box with an id around every detected person
import cv2
import supervision as sv
from kafka import KafkaConsumer, KafkaProducer
import pickle
import time
from config import KAFKA_SERVER
from prometheus_client import Summary, Histogram, start_http_server

LATENCY_SUMMARY = Summary('module_latency_p1', 'Latency of frames in the module')
HISTOGRAM = Histogram('hist_latency_p1', 'Histogram for module latency', buckets=[0.005, 0.01, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50])

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
            consumer = KafkaConsumer('m1_m2', value_deserializer=lambda v: pickle.loads(v), bootstrap_servers=[KAFKA_SERVER], group_id='module2', auto_offset_reset='earliest',)
            producer = KafkaProducer(bootstrap_servers=[KAFKA_SERVER], value_serializer=lambda v: pickle.dumps(v), max_request_size=100000000)
            print("Connected to Kafka broker")
            break
        except:
            time.sleep(3)
            pass

    person_annotator = PersonBoundingBoxAnnotator()

    for msg in consumer:
        frame = cv2.imdecode(msg.value['frame'], cv2.IMREAD_COLOR)
        detections = msg.value['detections']
        frame_count = msg.value['frame_count']
        hash = msg.value['hash']
        latency_start = msg.value['latency_start']

        if (frame_count == 0):
            data = {
                "frame": frame_encoded,
                "detections": detections,
                "frame_count": frame_count,
                "hash": hash
            }
            producer.send('m2_m3', value=data)

            break

        #Testing
        start_time = time.time()

        #print("Received frame:", frame_count)

        frame = person_annotator.annotate(frame=frame, detections=detections)

        frame_encoded = cv2.imencode('.jpg', frame)[1]

        # outputs
        # - frame
        # - detections
        data = {
            "frame": frame_encoded,
            "detections": detections,
            "frame_count": frame_count,
            "hash": hash,
            "latency_start": latency_start
        }
        
        #send json
        producer.send('m2_m3', value=data)
        #print("Sent frame:", frame_count)

        #Testing
        end_time = time.time()
        latency = end_time - start_time
        LATENCY_SUMMARY.observe(latency)
        HISTOGRAM.observe(latency)

    producer.flush()
    consumer.commit()

if __name__ == "__main__":
    start_http_server(8002)
    main()