import requests
from time import sleep

#sending to a REST HTTP endpoint using fastAPI @app.post()
resp = requests.post('http://127.0.0.1:8000/event/arrivals', json={'workflow_id':'wf','event_key':'my_key','event_payload':'testPayload'})

#sample format for submitting an external event to a REST HTTP endpoint using fastAPI's @app.get().
#resp = requests.get("http://127.0.0.1:8000/event/arrivals?workflow_id=wf&event_key=my_key&event_payload=testPayload")
print(resp.text)
