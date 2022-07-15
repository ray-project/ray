# __local_model_start__
# File name: local_model.py
from transformers import pipeline


def summarize(text):
    # Load model
    summarizer = pipeline("summarization", model="t5-small")

    # Run inference
    summary_list = summarizer(text)

    # Post-process output to return only the summary text
    summary = summary_list[0]["summary_text"]

    return summary


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

summary = summarize(article_text)
print(summary)
# __local_model_end__

assert summary == (
    "two astronauts steered their fragile lunar module safely "
    "and smoothly to the historic landing . the first men to reach the moon "
    "-- Armstrong and his co-pilot, col. Edwin E. Aldrin Jr. of the air force "
    "-- brought their ship to rest on a level, rock-strewn plain ."
)
