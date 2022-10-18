
import os
from time import sleep

import requests

import instaloader

POSTS_LIMIT = int(os.getenv("POSTS_LIMIT", '10'))
COMMENTS_LIMIT = int(os.getenv("COMMENTS_LIMIT", '20'))
USER_TEST = os.getenv("USER_TEST", "<user_here>")
PASS_TEST = os.getenv("PASS_TEST", "<pass_here>")
USER_TO_WATCH = os.getenv("USER_TO_WATCH", "matteorenzi, giorgiameloni").split(",")
LOGSTASH_URL = "http://logstash:9700"
PROJEJCT_ID = 'instap_id'


insta = instaloader.Instaloader()
insta.login(USER_TEST, PASS_TEST)
for user in USER_TO_WATCH:
    posts = instaloader.Profile.from_username(insta.context, user).get_posts()

    for i, post in enumerate(posts):
        if i == POSTS_LIMIT:
            break
        comments = post.get_comments()
        for index, comment in enumerate(comments):
            if index == COMMENTS_LIMIT:
                break

            data = {
                'message_id': index,
                'post_id': f'{post.owner_username}_{i}',
                'user': post.owner_username,
                'comment': comment.text,
                'caption': post.caption,
                'image': post.url,
                'timestamp': str(post.date_local),
                'likes': post.likes,
                'lat': post.location.lat if post.location else None,
                'lng': post.location.lng if post.location else None
            }
            print(str(data))
            sleep(1)
            x = requests.post(LOGSTASH_URL, json=data, timeout=5)
