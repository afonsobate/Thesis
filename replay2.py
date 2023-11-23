#Annotates each frame by putting a box with the id around the desired person
import cv2
import supervision as sv
from kafka import KafkaConsumer, KafkaProducer
import pickle
import time
from config import KAFKA_SERVER

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
            consumer = KafkaConsumer('r1_r2', value_deserializer=lambda v: pickle.loads(v), bootstrap_servers=[KAFKA_SERVER], group_id='module2', auto_offset_reset='earliest',)
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
        person_id = int(msg.value['person_id'])
        if (error == 1):
            data = {"error": msg.value['error']}
            producer.send("r2_r3", data)
            break

        elif (person_id == 0):
            data = {
                "person_id": person_id,
                "error": error
            }
            producer.send('r2_r3', value=data)
            break

        else:
            print("Received frame:", frame_count)

            frame = cv2.imdecode(msg.value['frame'], cv2.IMREAD_COLOR)
            detections = msg.value['detections']
            frame_number = int(msg.value['frame_number'])
            tracked = int(msg.value['tracked'])
            alert = int(msg.value['alert'])
            
            if (tracked == 1):
                frame = person_annotator.annotate(frame=frame, detections=detections)

            frame_encoded = cv2.imencode('.jpg', frame)[1]

            # outputs
            # - frame
            # - detections
            data = {
                "frame": frame_encoded,
                "detections": detections,
                "frame_number": frame_number,
                "person_id": person_id,
                "alert": alert,
                "error": error,
                "tracked": tracked
            }
            
            #send json
            producer.send('r2_r3', value=data)
            print("Sent frame:", frame_count)

            frame_count += 1

    producer.flush()
    consumer.commit()

if __name__ == "__main__":
    main()