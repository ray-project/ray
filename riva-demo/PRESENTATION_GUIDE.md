# Riva Models Demo - Presentation Guide

**Date:** 2026-05-16 (Friday)  
**Event:** Astra All-Hands  
**Duration:** 5 minutes  
**Audience:** Cross-team engineers (Astra org)

---

## Pre-Demo Checklist

- [ ] Test microphone access in the browser
- [ ] Verify `INFERENCE_API_KEY` is set and working
- [ ] Run `python test_api.py` to confirm API connectivity
- [ ] Start the demo app: `python app.py`
- [ ] Open browser to `http://localhost:5000`
- [ ] Prepare a simple test phrase (e.g., "Hello, how are you today?")
- [ ] Check audio output works on laptop

---

## Talk Structure (5 minutes)

### Minute 1: Introduction (60 seconds)

**What we shipped:**
- 5 Riva-family models now live on `inference-api.nvidia.com`
- Production gateway, ready for all teams to use

**The models:**
1. **Parakeet TDT 0.6B** - Fast ASR, 600M parameters
2. **Parakeet 1.1B CTC EN-US** - English-optimized ASR
3. **Parakeet 1.1B RNNT** - Multilingual ASR
4. **Magpie TTS 357M** - Multilingual text-to-speech
5. **Riva Translate 4B** - Neural machine translation

**Why it matters:**
- Standard OpenAI-compatible APIs
- No special SDK needed
- Same auth flow as other Inference Hub models

### Minutes 2-5: Live Demo (240 seconds)

#### Demo 1: Full Pipeline (120 seconds)
1. Open the app, show the clean UI
2. Click "Full Pipeline" tab
3. Explain the flow diagram:
   - 🎤 Record → 📝 Transcribe (ASR) → 🌍 Translate → 🔊 Synthesize (TTS)
4. **Record a phrase:** "Hello, how are you today?"
5. Click "Run Pipeline"
6. Watch results populate:
   - Transcribed: "Hello, how are you today?"
   - Translated: "Hola, ¿cómo estás hoy?"
   - Play synthesized Spanish audio
7. Emphasize: **3 models, 1 API call chain, ~2-3 seconds**

#### Demo 2: Compare ASR Models (60 seconds)
1. Switch to "ASR Only" tab
2. Show dropdown with 3 Parakeet variants
3. Record the same phrase
4. Transcribe with different models
5. Point out: Same API, different model IDs

#### Demo 3: Translation + TTS Individual (60 seconds)
1. Switch to "Translate Only" tab
2. Type: "The weather is nice today"
3. Translate English → French
4. Show result
5. Switch to "TTS Only" tab
6. Paste French translation
7. Synthesize and play

---

## Key Talking Points

### Technical Details
- **ASR Endpoint:** `POST /v1/audio/transcriptions` (OpenAI-compatible)
- **TTS Endpoint:** `POST /v1/audio/speech` (OpenAI-compatible)
- **Translation:** `POST /v1/chat/completions` (chat-completions pattern)

### What Makes This Cool
- **Pass-through models** - No LiteLLM wrapping, direct to NIM
- **Production-ready** - All 5 models have Datadog uptime probes
- **Easy onboarding** - Same API key, same auth, familiar patterns

### What Teams Can Build
- Voice assistants with multilingual support
- Real-time transcription + translation pipelines
- Content localization tools
- Accessibility features (TTS for text content)

---

## Backup Talking Points (If Extra Time)

### If Demo Fails
- Show the README and code structure
- Explain the API patterns on screen
- Reference the Datadog probes (all passing)

### If Asked About Performance
- ASR: ~1-2 seconds for 5-10 second audio clips
- Translation: ~500ms for typical sentences
- TTS: ~1-2 seconds for typical sentences
- Full pipeline: ~3-4 seconds end-to-end

### If Asked About Languages
- **Parakeet TDT/CTC:** English-focused
- **Parakeet RNNT:** Multilingual (20+ languages)
- **Magpie TTS:** Multilingual (10+ languages)
- **Riva Translate:** 100+ language pairs

### If Asked About Limitations
- HTTP request/response only (no WebSocket streaming yet)
- Audio format: WAV recommended, MP3/OGG supported
- Max audio length: ~30 seconds recommended for low latency

---

## Q&A Prep

**Q: Where do I get an API key?**  
A: Same as other Inference Hub models - standard NVIDIA API key.

**Q: Is this free?**  
A: Same pricing model as other Inference Hub endpoints.

**Q: Can I use this in production?**  
A: Yes! All 5 models are on the production gateway with uptime monitoring.

**Q: What about WebSocket/streaming?**  
A: Not yet proxied through the Hub. HTTP only for now (per INFH-13).

**Q: Why chat-completions for translation?**  
A: Riva Translate is exposed via the chat-completions pattern - same as other LLMs. See PASSTHROUGH-MODEL-PATTERN.md in inference-gway docs.

---

## Post-Demo

**Share the repo:**
- GitHub link: [TODO - add after creating repo]
- README has full setup instructions
- Takes 2 minutes to run locally

**Next steps:**
- Try it yourself with your use cases
- File issues/requests in the Inference Hub channels
- Check Datadog for model uptime

---

## Emergency Fallback

If the live demo completely fails:

1. Show the UI screenshots (pre-capture some)
2. Walk through the code structure in the repo
3. Show the API call patterns in `app.py`
4. Reference the Datadog probes (all green)
5. Emphasize: "This works, it's live, go try it!"

---

## Slide Deck Structure (Optional)

If creating slides:

1. **Title Slide:** Riva Models on Inference Hub
2. **The 5 Models:** Icons + names + purposes
3. **Live Demo:** (skip to live app)
4. **How It Works:** Architecture diagram
5. **Get Started:** Code snippet + repo link
6. **Q&A**

Keep slides minimal - the live demo is the star.
