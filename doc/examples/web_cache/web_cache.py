"""
The purpose of this module is to show how one could implement data
caching using Ray. If you tried to use multiprocessing to implement
this from scratch, you would need to deal with race conditions, 
thread-safety, and sharing memory between multiple processes. With
Ray, you can write this cache roughly like you would write a
single-threaded Python cache.
"""
import time
import requests
import ray
import flask
import time

ray.init()
app = flask.Flask(__name__)

# This ray.remote decorator defines WebCache as a Ray actor,
# rather than a normal Python class.
@ray.remote
class WebCache:
    def __init__(self):
        # A dict will serve as our cache
        self.users: Dict[int, any]  = {}

    def get_user(self, uid: int):
        return self.users.get(uid)

    def put_user(self, uid: int, user: any):
        self.users[uid] = user

@app.route('/get_user/<uid>')
def fetch_external_user_handler(uid):
    """This handler retrieves data from an external API if the 
       data isn't cached, otherwise it returns the cached data."""
    uid = int(uid)
    # Check whether the data we want is already cached.
    cache = ray.get_actor("web_cache")
    user = ray.get(cache.get_user.remote(uid))
    if user:
        return user

    # Otherwise fetch from API and update the cache
    user = fetch_user_from_api(uid)
    cache.put_user.remote(uid, user)
    return user

def fetch_user_from_api(uid: int):
    # Imagine this function is an expensive call to an external API
    time.sleep(5)
    return {"name": "max", "id": uid}

# This function is being run as a Ray task because the flask
# server is blocking, so we need to run this code from another process.
# Keeping these in the same file is for example purposes only.
@ray.remote
def test_web_cache():
    # sleep for a moment to make sure the server has started up.
    time.sleep(5) 
    # Request the same user id twice. We expect the first call to take
    # substantially longer because the value isn't cached yet.
    timer_st = time.monotonic()
    resp = requests.get('http://localhost:3000/get_user/4')
    timer_end = time.monotonic()
    first_req_runtime_s = timer_end - timer_st

    timer_st = time.monotonic()
    resp = requests.get('http://localhost:3000/get_user/4')
    timer_end = time.monotonic()
    second_req_runtime_s = timer_end - timer_st

    print(f"The first request took {first_req_runtime_s} seconds.")
    print(f"The second request took {second_req_runtime_s} seconds.")

if __name__ == "__main__":
    # This line instantiates a WebCache actor.
    # Rather than saving a handle to the actor, we use a "detached actor"
    # This is an actor that can be fetched by name, as opposed to having
    # to pass its handle around. The advantage here is that you can instantiate
    # an actor once at app initialization time and use it from all your
    # handlers without passing a new argument through to them.
    WebCache.options(name="web_cache", lifetime="detached").remote()
    test_web_cache.remote()
    app.run(host="localhost", port=3000)

