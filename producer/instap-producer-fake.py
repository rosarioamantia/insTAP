import os
from time import sleep
from datetime import datetime
import requests

TOPIC = os.getenv("KAFKA_TOPIC", "instap")
USER_TO_WATCH = os.getenv("USER_TO_WATCH", "chiaraferragni")

LOGSTASH_URL = "http://logstash:9700"
PROJEJCT_ID = 'instap_id'

i = 0
while i < 1000000000:
    sleep(10)
    data = {
        'id': i,
        'user': USER_TO_WATCH,
        'comment': f'commento n° {str(i)}',
        'caption': f'caption n° {str(i)}',
        'image': "post_image",
        'timestamp': str(datetime.utcnow().strftime("%m/%d/%Y, %H:%M:%S")),
        'lat': 12.3,
        'lng': 30.12
    }
    x = requests.post(LOGSTASH_URL, json=data, timeout=5)
    print(str(data))
    sleep(5)
    i += 1
