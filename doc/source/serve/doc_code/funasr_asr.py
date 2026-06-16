# __example_code_start__
import contextlib
import os
import tempfile
from typing import Any, Iterable, List, Optional

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import PlainTextResponse

from ray import serve
from ray.serve.handle import DeploymentHandle


fastapi_app = FastAPI()
DEFAULT_MODEL = "iic/SenseVoiceSmall"
ALLOWED_MODELS = {DEFAULT_MODEL}


@serve.deployment(
    autoscaling_config={
        "min_replicas": 1,
        "initial_replicas": 1,
        "max_replicas": 2,
        "target_ongoing_requests": 20,
    },
    max_ongoing_requests=40,
)
@serve.ingress(fastapi_app)
class OpenAICompatibleIngress:
    def __init__(self, asr_handle: DeploymentHandle):
        self._asr_handle = asr_handle

    @fastapi_app.post("/v1/audio/transcriptions")
    async def create_transcription(
        self,
        file: UploadFile = File(...),
        model: str = Form(DEFAULT_MODEL),
        language: Optional[str] = Form(None),
        response_format: str = Form("json"),
    ):
        if model not in ALLOWED_MODELS:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Unsupported model '{model}'. Supported models: "
                    f"{sorted(ALLOWED_MODELS)}"
                ),
            )

        if response_format not in {"json", "text", "verbose_json"}:
            raise HTTPException(
                status_code=400,
                detail=(
                    "response_format must be one of: json, text, verbose_json"
                ),
            )

        audio_bytes = await file.read()
        asr_handle = self._asr_handle.options(multiplexed_model_id=model)
        result = await asr_handle.transcribe.remote(
            audio_bytes, file.filename, language, response_format
        )

        if response_format == "text":
            return PlainTextResponse(result["text"])
        return result


@serve.deployment(
    autoscaling_config={
        "min_replicas": 0,
        "initial_replicas": 1,
        "max_replicas": 4,
        "target_ongoing_requests": 4,
    },
    max_ongoing_requests=8,
    ray_actor_options={"num_gpus": 1},
)
class FunASRModel:
    def __init__(
        self,
        default_model: str = DEFAULT_MODEL,
        allowed_models: Iterable[str] = tuple(ALLOWED_MODELS),
        device: Optional[str] = None,
    ):
        import torch
        from funasr import AutoModel
        from funasr.utils.postprocess_utils import rich_transcription_postprocess

        self._device = device or ("cuda:0" if torch.cuda.is_available() else "cpu")
        self._default_model = default_model
        self._allowed_models = set(allowed_models)
        self._auto_model_cls = AutoModel
        self._postprocess = rich_transcription_postprocess

    @serve.multiplexed(max_num_models_per_replica=1)
    async def _get_model(self, model_name: str) -> Any:
        if model_name not in self._allowed_models:
            raise ValueError(
                f"Unsupported model '{model_name}'. Supported models: "
                f"{sorted(self._allowed_models)}"
            )

        return self._auto_model_cls(
            model=model_name,
            vad_model="fsmn-vad",
            vad_kwargs={"max_single_segment_time": 30000},
            device=self._device,
        )

    @serve.batch(max_batch_size=4, batch_wait_timeout_s=0.1)
    async def transcribe(
        self,
        audio_bytes: List[bytes],
        filenames: List[Optional[str]],
        languages: List[Optional[str]],
        response_formats: List[str],
    ) -> List[dict]:
        model_name = serve.get_multiplexed_model_id() or self._default_model
        model = await self._get_model(model_name)
        audio_paths = []
        try:
            for audio, filename in zip(audio_bytes, filenames):
                suffix = os.path.splitext(filename or "")[1] or ".wav"
                with tempfile.NamedTemporaryFile(
                    suffix=suffix, delete=False
                ) as audio_file:
                    audio_file.write(audio)
                    audio_paths.append(audio_file.name)

            results = model.generate(
                input=audio_paths,
                language=[language or "auto" for language in languages],
                use_itn=True,
                batch_size_s=60,
                merge_vad=True,
                merge_length_s=15,
            )
        finally:
            for audio_path in audio_paths:
                with contextlib.suppress(FileNotFoundError):
                    os.remove(audio_path)

        responses = []
        for transcription, response_format in zip(results, response_formats):
            text = self._postprocess(transcription.get("text", ""))
            if response_format == "verbose_json":
                responses.append(
                    {
                        "text": text,
                        "segments": transcription.get("sentence_info", []),
                        "model": model_name,
                    }
                )
            else:
                responses.append({"text": text})
        return responses


entrypoint = OpenAICompatibleIngress.bind(FunASRModel.bind())
# __example_code_end__
