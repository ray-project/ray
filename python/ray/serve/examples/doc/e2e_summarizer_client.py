# __client_class_start__
# File name: summarizer_client.py
import requests

article_text = (
    "HOUSTON -- Men have landed and walked on the moon. "
    "Two Americans, astronauts of Apollo 11, steered their fragile "
    "four-legged lunar module safely and smoothly to the historic landing "
    "yesterday at 4:17:40 P.M., Eastern daylight time. Neil A. Armstrong, the "
    "38-year-old commander, radioed to earth and the mission control room "
    'here: "Houston, Tranquility Base here. The Eagle has landed." The '
    "first men to reach the moon -- Armstrong and his co-pilot, Col. Edwin E. "
    "Aldrin Jr. of the Air Force -- brought their ship to rest on a level, "
    "rock-strewn plain near the southwestern shore of the arid Sea of "
    "Tranquility. About six and a half hours later, Armstrong opened the "
    "landing craft's hatch, stepped slowly down the ladder and declared as "
    "he planted the first human footprint on the lunar crust: \"That's one "
    'small step for man, one giant leap for mankind." His first step on the '
    "moon came at 10:56:20 P.M., as a television camera outside the craft "
    "transmitted his every move to an awed and excited audience of hundreds "
    "of millions of people on earth."
)

response = requests.get("http://127.0.0.1:8000/Summarizer?txt=" + article_text).text

print(response)
# __client_class_end__
