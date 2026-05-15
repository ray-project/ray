# Riva Models Demo - Quick Reference

**Presentation Date:** Friday 2026-05-16  
**Duration:** 5 minutes  
**Repo Location:** `/workspace/riva-demo` (or [GitHub PR #63355](https://github.com/ray-project/ray/pull/63355))  
**Linear Issue:** [INFH-53](https://linear.app/nvidia/issue/INFH-53)

---

## 📋 Pre-Presentation Checklist

### 15 Minutes Before
- [ ] Set `INFERENCE_API_KEY` environment variable
- [ ] Run `./setup.sh` to verify everything works
- [ ] Start app: `python3 app.py`
- [ ] Open browser: `http://localhost:5000`
- [ ] Test microphone access (grant permissions)
- [ ] Do one full pipeline test recording
- [ ] Verify audio playback works on laptop speakers

### 5 Minutes Before
- [ ] Have browser window ready with demo open
- [ ] Close unnecessary browser tabs
- [ ] Check laptop volume is audible
- [ ] Have backup plan ready (screenshots/code)
- [ ] PRESENTATION_GUIDE.md open in another window

---

## 🎯 The 5 Models (30 seconds)

1. **Parakeet TDT 0.6B** - Fast ASR (600M params)
2. **Parakeet 1.1B CTC EN-US** - English ASR
3. **Parakeet 1.1B RNNT** - Multilingual ASR
4. **Magpie TTS 357M** - Multilingual TTS
5. **Riva Translate 4B** - Neural translation

**Why it matters:** OpenAI-compatible APIs, production-ready, no special SDK needed.

---

## 🚀 Demo Script (4 minutes)

### Demo 1: Full Pipeline (2 min)
1. Show main UI - point out 5 model cards
2. Click "Full Pipeline" tab
3. Explain flow: 🎤 Record → 📝 ASR → 🌍 Translate → 🔊 TTS
4. **Record:** "Hello, how are you today?"
5. Click "Run Pipeline"
6. Show results:
   - Transcription
   - Translation (Spanish)
   - **Play audio** - synthesized Spanish speech
7. Say: "3 models, one flow, under 5 seconds"

### Demo 2: Compare ASR Models (1 min)
1. Switch to "ASR Only" tab
2. Show dropdown with 3 Parakeet models
3. Record same phrase
4. Transcribe with TDT 0.6B
5. Say: "Same API, different models - choose based on your needs"

### Demo 3: Individual Models (1 min)
1. "Translate Only" - type + translate EN→FR
2. "TTS Only" - synthesize French text
3. Say: "Each model accessible individually via standard APIs"

---

## 💬 Key Talking Points

- **Production Ready**: All 5 models live on inference-api.nvidia.com
- **Standard APIs**: OpenAI-compatible `/v1/audio/*` and `/v1/chat/completions`
- **No SDK Needed**: Simple REST calls with bearer token
- **Monitored**: Datadog uptime probes on all 5 models
- **Easy to Start**: "You could build something with these Monday morning"

---

## 🆘 Backup Plans

### If Microphone Fails
→ Switch to Translation + TTS tabs (no recording needed)

### If Network Fails
→ Show code in `app.py`, explain API patterns

### If Full Pipeline Errors
→ Show individual tabs working separately

### If Everything Fails
→ "These are live, monitored in Datadog, go try it yourself!"

---

## 📞 Quick Links

- **GitHub PR:** https://github.com/ray-project/ray/pull/63355
- **Datadog Probes:** [View 4146189](https://nvitprod.datadoghq.com/synthetics/tests?saved-view-id=4146189)
- **Linear Issue:** [INFH-53](https://linear.app/nvidia/issue/INFH-53)
- **Full Guide:** See `PRESENTATION_GUIDE.md` in this directory

---

## 📝 Post-Demo

Update Linear INFH-53 with:
- [ ] Link to this repo/PR
- [ ] Recording link (if available)
- [ ] Any audience questions/feedback
- [ ] Follow-up tickets if needed

---

## 🎤 Test Phrases

**English:**
- "Hello, how are you today?"
- "The weather is beautiful this morning."
- "I'm excited to learn about these models."

**For Translation:**
- EN → ES: "Hello" → "Hola"
- EN → FR: "Thank you" → "Merci"
- EN → DE: "Good morning" → "Guten Morgen"

---

**Last Updated:** 2026-05-15  
**Status:** ✅ Demo Ready  
**Confidence Level:** High - app tested and working
