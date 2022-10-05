import os
from time import sleep
from datetime import datetime
import requests
import random

TOPIC = os.getenv("KAFKA_TOPIC", "instap")
USER_TO_WATCH = os.getenv("USER_TO_WATCH", "chiaraferragni")

LOGSTASH_URL = "http://logstash:9700"
PROJEJCT_ID = 'instap_id'

positive_comments = ["You're beautiful!!", "I love you!", "WOOOOW!", "Perfect!"]

negative_comments = ["I hate you!", "I don't like this." , "too bad"]

i = 0
while i < 1000000000:
    rand_idx = random.randrange(3)
    coin = random.randrange(2)
    data = {
        'id': i,
        'user': USER_TO_WATCH,
        'comment': positive_comments[rand_idx] if coin %2 else negative_comments[rand_idx],
        'caption': f'caption n° {str(i)}',
        'image': "https://images.newscientist.com/wp-content/uploads/2021/06/03141753/03-june_puppies.jpg",
        'timestamp': str(datetime.utcnow().strftime("%m/%d/%Y, %H:%M:%S")),
        'location': {'lat': 12.3, 'lon': 30.12 }
    }
    x = requests.post(LOGSTASH_URL, json=data, timeout=5)
    print(str(data))
    sleep(5)
    i += 1
