# Riva Models Showcase Demo

**5-minute demo for Astra all-hands 2026-05-16**

Live demonstration of 5 Riva-family speech models on `inference-api.nvidia.com`:

1. **Parakeet TDT 0.6B** - ASR (Automatic Speech Recognition)
2. **Parakeet 1.1B CTC EN-US** - ASR (English)
3. **Parakeet 1.1B RNNT Multilingual** - ASR (Multilingual)
4. **Magpie TTS Multilingual 357M** - Text-to-Speech
5. **Riva Translate 4B** - Neural Machine Translation (via chat-completions)

## Quick Start

### Prerequisites

- Python 3.8+
- NVIDIA Inference API key

### Setup

1. Clone and navigate to the demo directory:
```bash
cd riva-demo
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set your API key:
```bash
export INFERENCE_API_KEY="your-api-key-here"
```

4. Run the app:
```bash
python app.py
```

5. Open your browser to:
```
http://localhost:5000
```

## Demo Flow

The app provides 4 interactive tabs:

### 🚀 Full Pipeline
Complete end-to-end flow:
- Record audio in your browser
- Transcribe with any Parakeet ASR model
- Translate with Riva Translate 4B
- Synthesize translation with Magpie TTS
- Play back the result

**Example:** Say "Hello, how are you?" in English → transcribe → translate to Spanish → hear "Hola, ¿cómo estás?"

### 🎤 ASR Only
Test any of the three Parakeet models individually:
- Record or upload audio
- Compare transcription quality across models
- See which works best for your use case

### 🌍 Translation Only
Direct translation testing:
- Type text manually
- Translate between English, Spanish, French, German
- Powered by Riva Translate 4B via chat-completions API

### 🔊 TTS Only
Text-to-speech synthesis:
- Type any text
- Generate natural-sounding speech
- Powered by Magpie TTS Multilingual 357M

## Architecture

```
Browser                   Flask App                 inference-api.nvidia.com
  │                          │                              │
  │──record audio──>         │                              │
  │                          │──POST /v1/audio/transcriptions──>
  │                          │         (Parakeet ASR)       │
  │                          │<──────transcribed text──────│
  │                          │                              │
  │                          │──POST /v1/chat/completions──>
  │                          │      (Riva Translate 4B)     │
  │                          │<──────translated text───────│
  │                          │                              │
  │                          │──POST /v1/audio/speech──────>
  │                          │        (Magpie TTS)          │
  │                          │<──────audio data────────────│
  │<──play audio──          │                              │
```

## API Endpoints

The Flask backend exposes:

- `GET /` - Main demo UI
- `POST /api/transcribe` - ASR transcription
- `POST /api/translate` - Text translation
- `POST /api/synthesize` - Text-to-speech
- `POST /api/full-pipeline` - Complete ASR → Translation → TTS flow
- `GET /api/health` - Health check

## Model Details

### Parakeet ASR Models
All three variants support the OpenAI-compatible `/v1/audio/transcriptions` endpoint:
- **TDT 0.6B**: Fast, compact model
- **CTC 1.1B EN-US**: Optimized for English
- **RNNT 1.1B**: Multilingual support

### Riva Translate 4B
Translation via chat-completions API pattern:
```python
POST /v1/chat/completions
{
  "model": "nvidia/riva-translate-4b",
  "messages": [{"role": "user", "content": "Translate from English to Spanish: Hello"}]
}
```

### Magpie TTS 357M
OpenAI-compatible `/v1/audio/speech` endpoint:
```python
POST /v1/audio/speech
{
  "model": "nvidia/magpie-tts-multilingual-357m",
  "input": "Text to synthesize",
  "voice": "default"
}
```

## Troubleshooting

**"INFERENCE_API_KEY not set"**
- Make sure you've exported the environment variable
- Check: `echo $INFERENCE_API_KEY`

**"Microphone access denied"**
- Grant microphone permissions in your browser
- Try HTTPS if on a remote server (required for getUserMedia)

**Network errors**
- Verify your API key is valid
- Check internet connection
- Ensure `inference-api.nvidia.com` is reachable

## For the Presentation

**Talking points (1 minute):**
- 5 models live on NVCF production gateway
- ASR: 3 Parakeet variants (different sizes/languages)
- Translation: Riva Translate 4B via chat-completions
- TTS: Magpie multilingual synthesis
- All via standard OpenAI-compatible APIs

**Demo script (4 minutes):**
1. Show the main UI (0:30)
2. Full pipeline demo - record → transcribe → translate → TTS (2:00)
3. Compare ASR models side-by-side (1:00)
4. Show translation + TTS individually (0:30)

## Source References

Related infrastructure:
- Datadog probes: [Saved view 4146189](https://nvitprod.datadoghq.com/synthetics/tests?saved-view-id=4146189)
- Probe config: `inference-skills/manage-synthetic-probes/probes/riva-prd.yaml`
- Smoke tests: `inference-gway/tests/smoke-test-riva-family.py`
- Linear epic: [INFH-5](https://linear.app/nvidia/issue/INFH-5)

## License

Internal demo for Astra all-hands 2026-05-16
