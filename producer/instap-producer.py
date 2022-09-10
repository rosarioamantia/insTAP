import os
from json import dumps
from time import sleep
import requests

import instaloader

POSTS_LIMIT = os.getenv( "POSTS_LIMIT", 10)
COMMENTS_LIMIT = os.getenv("COMMENTS_LIMIT", 10)
USER_TEST = os.getenv("USER_TEST","<user_here>")
PASS_TEST = os.getenv("PASS_TEST","<pass_here>")
USER_TO_WATCH = os.getenv("USER_TO_WATCH", "chiaraferragni")
LOGSTASH_URL = "http://logstash:9600"
PROJEJCT_ID = 'instap_id'

insta = instaloader.Instaloader()
insta.login(USER_TEST, PASS_TEST)

posts = instaloader.Profile.from_username(insta.context, USER_TO_WATCH).get_posts()
for i, post in enumerate(posts):
    if i == POSTS_LIMIT:
        break
    comments = post.get_comments()
    for index, comment in enumerate(comments):
        if index == COMMENTS_LIMIT:
            break
        data = {'user': USER_TO_WATCH, 'comment': comment.text}
        print(str(data))
        x = requests.post(LOGSTASH_URL, json = data, timeout=2.50)
        sleep(5)
