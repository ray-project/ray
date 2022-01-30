import atoma
from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import sqlite3

import ray


@ray.remote
class NewsServer(object):
    def __init__(self):
        self.conn = sqlite3.connect("newsreader.db")
        c = self.conn.cursor()
        c.execute(
            """CREATE TABLE IF NOT EXISTS news
                     (title text, link text,
                     description text, published timestamp,
                     feed url, liked bool)"""
        )
        self.conn.commit()

    def retrieve_feed(self, url):
        response = requests.get(url)
        feed = atoma.parse_rss_bytes(response.content)
        items = []
        c = self.conn.cursor()
        for item in feed.items:
            items.append(
                {
                    "title": item.title,
                    "link": item.link,
                    "description": item.description,
                    "description_text": item.description,
                    "pubDate": str(item.pub_date),
                }
            )
            c.execute(
                """INSERT INTO news (title, link, description,
                         published, feed, liked) values
                         (?, ?, ?, ?, ?, ?)""",
                (
                    item.title,
                    item.link,
                    item.description,
                    item.pub_date,
                    feed.link,
                    False,
                ),
            )
        self.conn.commit()

        return {
            "channel": {"title": feed.title, "link": feed.link, "url": feed.link},
            "items": items,
        }

    def like_item(self, url, is_faved):
        c = self.conn.cursor()
        if is_faved:
            c.execute("UPDATE news SET liked = 1 WHERE link = ?", (url,))
        else:
            c.execute("UPDATE news SET liked = 0 WHERE link = ?", (url,))
        self.conn.commit()


# instantiate the app
app = Flask(__name__)
app.config.from_object(__name__)

# enable CORS
CORS(app)


@app.route("/api", methods=["POST"])
def dispatcher():
    req = request.get_json()
    method_name = req["method_name"]
    method_args = req["method_args"]
    if hasattr(dispatcher.server, method_name):
        method = getattr(dispatcher.server, method_name)
        # Doing a blocking ray.get right after submitting the task
        # might be bad for performance if the task is expensive.
        result = ray.get(method.remote(*method_args))
        return jsonify(result)
    else:
        return jsonify({"error": "method_name '" + method_name + "' not found"})


if __name__ == "__main__":
    ray.init(num_cpus=2)
    dispatcher.server = NewsServer.remote()
    app.run()
