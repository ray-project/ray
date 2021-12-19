import asyncio
import time

import ray
import ray.experimental.aio as rayaio

ray.init()


@ray.remote
class EnglishSpeechToTextModel:
    def run(self, speech):
        print("    ... Entering EnglishSpeechToTextModel", flush=True)
        time.sleep(1)
        return speech + " -(EnglishSpeechToText)->"


@ray.remote
class QuestionAnswerModel:
    def run(self, text):
        print("    ... Entering QuestionAnswerModel", flush=True)
        time.sleep(1)
        return text + " -(QuestionAnswer)->"


@ray.remote
class SemanticSearchModel:
    def run(self, text):
        print("    ... Entering SemanticSearchModel", flush=True)
        time.sleep(1)
        return text + " -(SemanticSearch)->"


@ray.remote
class EnglishToSpanishTranslationModel:
    def run(self, text):
        print("    ... Entering EnglishToSpanishTranslationModel", flush=True)
        time.sleep(1)
        return text + " -(EnglishToSpanishTranslation)->"


@ray.remote
class EnglishTextToSpeechModel:
    def run(self, text):
        print("    ... Entering EnglishTextToSpeechModel", flush=True)
        time.sleep(1)
        return text + " -(EnglishTextToSpeech)->"


@ray.remote
class SpanishTextToSpeechModel:
    def run(self, text):
        print("    ... Entering SpanishTextToSpeechModel", flush=True)
        time.sleep(1)
        return text + " -(SpanishTextToSpeech)->"


english_speech_to_text = EnglishSpeechToTextModel.remote()
question_and_answer = QuestionAnswerModel.remote()
semantic_search = SemanticSearchModel.remote()
english_to_spanish_translation = EnglishToSpanishTranslationModel.remote()
english_text_to_speech = EnglishTextToSpeechModel.remote()
spanish_text_to_speech = SpanishTextToSpeechModel.remote()


async def voice_assistant(question_speech):
    text = await english_speech_to_text.run.remote(question_speech)
    print("    ... Processed by EnglishSpeechToTextModel", flush=True)

    if "what" in text:
        search_result = await semantic_search.run.remote(text)
        print("    ... Processed by SemanticSearchModel", flush=True)
        return search_result

    question = text + " -(ExtractQuestion)->"
    answer = await question_and_answer.run.remote(question)
    print("    ... Processed by QuestionAnswerModel", flush=True)

    answer_speech = await english_text_to_speech.run.remote(answer)
    print("    ... Processed by EnglishTextToSpeechModel", flush=True)
    return answer_speech


async def spanish_translator(english_speech):
    text = await english_speech_to_text.run.remote(english_speech)
    text += " -(SwapUncommonWords)->"
    translation = await english_to_spanish_translation.run.remote(text)
    translation += " -(AdaptToDialect)->"
    return await spanish_text_to_speech.run.remote(translation)


print("")
print("\033[1mRunning voice assistant with asyncio:\033[0m")
coro = voice_assistant("hi siri")
print(f"asyncio: {asyncio.run(coro)}", flush=True)


print("")
print("\033[1mRunning voice assistant with distributed coroutine:\033[0m")
coro = voice_assistant("hi siri")
print(f"rayaio:  {asyncio.run(rayaio.run(coro))}", flush=True)
