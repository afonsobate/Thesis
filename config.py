from prometheus_client import start_http_server, Summary, Counter, Histogram

VIDEO_PATH = 'video/video_shorter.webm'

# yolov8n.pt, yolov8s.pt, yolov8m.pt, yolov8l.pt, yolov8x.pt (smallest to largest)
MODEL = 'yolov8n.pt'

VIDEO_RESOLUTION_WH = (960, 540)
VIDEO_FPS = 25
BLOCK_SIZE = 65536
KAFKA_SERVER = '192.168.1.76:9092'

SUMMARY_P1 = Summary('p1_latency', 'Time spent processing alert')
HISTOGRAM_P1 = Histogram('p1_hist_latency', 'Histogram for pipeline latency', buckets=[0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])
SUMMARY_P2 = Summary('p2_latency', 'Time spent processing alert')
COUNTER_P2 = Counter('p2_alert_counter', 'Alerts processed by the pipeline')
HISTOGRAM_P2 = Histogram('p2_hist_latency', 'Histogram for pipeline latency', buckets=[0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])