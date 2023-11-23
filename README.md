# V1

V1 version of smart retail application.

## Features

- Detects and tracks people in a video stream.
- Detects when a person walks in and out of a zone.
  - Registers the frame number of entry and exit.
  - Saves an image of the frame.
- Divided into 3 modules:
  - `module1.py`: Detects and tracks people. Sends detections and frames to the next module.  
  - `module2.py`: Annotates bounding boxes around people. Sends annotations and detections to the next module.
  - `module3.py`: Annotates zones. Detects when a person enters and exits a zone. Saves an image of the frame and writes to a csv.

## Details

- Uses [YOLOv8](https://github.com/ultralytics/ultralytics) for object detection and tracking (built-in [BoT-SORT](https://github.com/NirAharon/BoT-SORT)).
- [Supervision](https://github.com/roboflow/supervision) for zone detection and annotations.
- [Apache Kafka](https://kafka.apache.org/) for message passing.

## Setup

Tested with Python 3.10.7.

1. Create a virtual environment:

    ```shell
    python3 -m venv .venv
    source .venv/bin/activate
    ```

2. Install dependencies:

    ```shell
    pip3 install ultralytics supervision lap kafka-python pickles
    ```

## Running

1. Start Kafka server.

1. Configure [config.py](config.py).

1. Set `VENV_PATH` in [start.py](start.py) and run:

```shell
python3 start.py
```

Alternatively, run each module in a separate terminal:

```shell
python3 module1.py
python3 module2.py
python3 module3.py
```

## Config

The config.py file contains the following variables:

- `VIDEO_PATH`: Path to the video file.

- `VIDEO_RESOLUTION_WH`: Video resolution in width and height.

- `VIDEO_FPS`: Video framerate.

- `KAFKA_SERVER`: Kafka server address.

- `MODEL`: One of the following (see [YOLOv8 Models](https://github.com/ultralytics/ultralytics?ref=roboflow-blog#models)):
  - `yolov8n.pt`
  - `yolov8s.pt`
  - `yolov8m.pt`
  - `yolov8l.pt`
  - `yolov8x.pt`
