# __example_code_start__
import contextlib
import os
import tempfile
from typing import Any, Dict, Iterable, Optional

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import PlainTextResponse

from ray import serve
from ray.serve.handle import DeploymentHandle


fastapi_app = FastAPI()
DEFAULT_MODEL = "iic/SenseVoiceSmall"
ALLOWED_MODELS = {DEFAULT_MODEL}


@serve.deployment
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
        result = await self._asr_handle.transcribe.remote(
            audio_bytes=audio_bytes,
            filename=file.filename,
            model=model,
            language=language,
            response_format=response_format,
        )

        if response_format == "text":
            return PlainTextResponse(result["text"])
        return result


@serve.deployment(
    autoscaling_config={"min_replicas": 0, "initial_replicas": 1, "max_replicas": 4},
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
        self._models: Dict[str, Any] = {}
        self._default_model = default_model
        self._allowed_models = set(allowed_models)
        self._auto_model_cls = AutoModel
        self._postprocess = rich_transcription_postprocess

    def _get_model(self, model_name: str):
        if model_name not in self._allowed_models:
            raise ValueError(
                f"Unsupported model '{model_name}'. Supported models: "
                f"{sorted(self._allowed_models)}"
            )

        if model_name not in self._models:
            self._models[model_name] = self._auto_model_cls(
                model=model_name,
                vad_model="fsmn-vad",
                vad_kwargs={"max_single_segment_time": 30000},
                device=self._device,
            )
        return self._models[model_name]

    def transcribe(
        self,
        audio_bytes: bytes,
        filename: Optional[str],
        model: Optional[str],
        language: Optional[str],
        response_format: str,
    ):
        suffix = os.path.splitext(filename or "")[1] or ".wav"
        audio_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as audio_file:
                audio_file.write(audio_bytes)
                audio_path = audio_file.name

            model_name = model or self._default_model
            result = self._get_model(model_name).generate(
                input=audio_path,
                language=language or "auto",
                use_itn=True,
                batch_size_s=60,
                merge_vad=True,
                merge_length_s=15,
            )
        finally:
            if audio_path:
                with contextlib.suppress(FileNotFoundError):
                    os.remove(audio_path)

        transcription = result[0] if result else {}
        text = self._postprocess(transcription.get("text", ""))

        if response_format == "verbose_json":
            return {
                "text": text,
                "segments": transcription.get("sentence_info", []),
                "model": model_name,
            }
        return {"text": text}


entrypoint = OpenAICompatibleIngress.bind(FunASRModel.bind())
# __example_code_end__
