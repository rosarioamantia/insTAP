'''import os
from json import dumps
from time import sleep
import requests

import instaloader

POSTS_LIMIT = os.getenv("POSTS_LIMIT", 50)
COMMENTS_LIMIT = os.getenv("COMMENTS_LIMIT", 50)
USER_TEST = os.getenv("USER_TEST", "<user_here>")
PASS_TEST = os.getenv("PASS_TEST", "<pass_here>")
USER_TO_WATCH = os.getenv("USER_TO_WATCH", "chiaraferragni")
LOGSTASH_URL = "http://logstash:9700"
PROJEJCT_ID = 'instap_id'

insta = instaloader.Instaloader()
insta.login(USER_TEST, PASS_TEST)

posts = instaloader.Profile.from_username(insta.context, USER_TO_WATCH).get_posts()
for i, post in enumerate(posts):
    if i == POSTS_LIMIT:
        break
    post_caption = post.caption
    post_image_url = post.url
    comments = post.get_comments()
    comments_to_send = []
    for index, comment in enumerate(comments):
        if index == COMMENTS_LIMIT:
            break
        comments_to_send.append(comment.text)

    data = {
        'id': i,
        'user': USER_TO_WATCH,
        'comments': ", ".join(comments_to_send),
        'caption': post_caption,
        'pic_url': post_image_url
    }
    print(str(data))
    x = requests.post(LOGSTASH_URL, json=data, timeout=5)'''

import os
from json import dumps
from time import sleep

import instaloader
from kafka import KafkaProducer

TOPIC = os.getenv("KAFKA_TOPIC", "instap")
#POSTS_LIMIT = os.getenv( "POSTS_LIMIT", 10)
#COMMENTS_LIMIT = os.getenv("COMMENTS_LIMIT", 10)
#USER_TEST = os.getenv("USER_TEST","sentimentoanalisi")
#PASS_TEST = os.getenv("PASS_TEST","Ciao12345")
#USER_TO_WATCH = os.getenv("USER_TO_WATCH", "chiaraferragni")


producer = KafkaProducer(bootstrap_servers=['broker:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))
#insta = instaloader.Instaloader()
#insta.login(USER_TEST, PASS_TEST)

i = 0
while i < 1000000000:
    data = {'user': "ciao", 'id': i}
    producer.send(TOPIC, value=data)
    sleep(5)
    i+=1
