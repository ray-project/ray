from ray import serve
import re
import subprocess
from typing import List
import starlette.requests


def hard_normalize(word):
    """Lower case the word and remove all non-alpha-numeric characters
    from the entire word.
    """

    non_alpha_numeric = re.compile(r"[\W]+")
    return non_alpha_numeric.sub("", word.lower())


def clean_whisper_alignments(whisper_word_alignments: List[dict]) -> List[dict]:
    """Change required to match gentle's tokenization with Whisper's word alignments"""

    processed_words = []
    for word_alignment in whisper_word_alignments:
        if word_alignment.word == "%":
            processed_words.append(word_alignment._replace(word=" percent"))
        elif word_alignment.word[0] == "'" and len(processed_words) > 0:
            # eg: "'Or" from ["d", "'Or"]
            processed_words[-1]._replace(
                word=processed_words[-1].word + word_alignment.word,
                end=word_alignment.end,
            )
        elif hard_normalize(word_alignment.word) == "":
            # eg: " -"
            continue
        else:
            processed_words.append(word_alignment)

    return processed_words


@serve.deployment(ray_actor_options={"num_cpus": 1.0, "num_gpus": 1})
class WhisperModel:
    def __init__(self, model_size="large-v2"):
        # Load model
        from faster_whisper import WhisperModel

        # Run on GPU with FP16
        self.model = WhisperModel(model_size, device="cuda", compute_type="float16")

    async def transcribe(self, file_path: str):
        subprocess.check_call(["curl", "-o", "audio.mp3", "-sSfLO", file_path])

        segments, info = self.model.transcribe(
            "audio.mp3",
            language="en",
            initial_prompt="Here is the um, uh, Um, Uh, transcript.",
            best_of=5,
            beam_size=5,
            word_timestamps=True,
        )

        whisper_alignments = []
        transcript_text = ""
        for seg in segments:
            transcript_text += seg.text
            whisper_alignments += clean_whisper_alignments(seg.words)

        # Transcript change required to match gentle's tokenization with
        # Whisper's word alignments
        transcript_text = transcript_text.replace("% ", " percent ")
        return {
            "language": info.language,
            "language_probability": info.language_probability,
            "duration": info.duration,
            "transcript_text": transcript_text,
            "whisper_alignments": whisper_alignments,
        }

    async def __call__(self, req: starlette.requests.Request):
        request = await req.json()
        return await self.transcribe(file_path=request["filepath"])


entrypoint = WhisperModel.bind()
