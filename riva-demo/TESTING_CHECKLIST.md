# Testing Checklist for Riva Models Demo

## Pre-Demo Testing (Before All-Hands)

### Environment Setup
- [ ] Python 3.8+ installed
- [ ] Dependencies installed: `pip install -r requirements.txt`
- [ ] `INFERENCE_API_KEY` environment variable set
- [ ] Test API connectivity: `python3 test_api.py`
- [ ] All API tests pass (Translation + TTS minimum)

### Application Startup
- [ ] App starts without errors: `python3 app.py`
- [ ] No import errors or missing dependencies
- [ ] Server accessible at `http://localhost:5000`
- [ ] Browser can load the main page
- [ ] All 5 model cards display correctly

### Browser Compatibility
- [ ] Chrome/Chromium (recommended)
- [ ] Firefox
- [ ] Safari
- [ ] Edge

### Microphone Access
- [ ] Browser requests microphone permission
- [ ] Permission granted successfully
- [ ] Audio recording starts when clicking "Start Recording"
- [ ] Recording stops when clicking "Stop Recording"
- [ ] No console errors during recording

---

## Functional Testing

### Tab 1: Full Pipeline 🚀
**Test Case:** Complete ASR → Translation → TTS flow

**Steps:**
1. Navigate to "Full Pipeline" tab
2. Select ASR model (try each one):
   - Parakeet TDT 0.6B
   - Parakeet 1.1B CTC EN-US
   - Parakeet 1.1B RNNT Multilingual
3. Select translation languages (e.g., English → Spanish)
4. Click "Start Recording"
5. Speak clearly: "Hello, how are you today?"
6. Click "Stop Recording"
7. Click "Run Pipeline"

**Expected Results:**
- [ ] Loading indicator appears
- [ ] Transcribed text matches spoken words
- [ ] Translation appears in target language
- [ ] Audio player appears with synthesized speech
- [ ] Audio plays correctly when clicked
- [ ] No error messages
- [ ] Total time < 10 seconds

**Test Phrases:**
- English: "Hello, how are you today?"
- English: "The weather is beautiful this morning."
- English: "I'm excited to learn about these models."

### Tab 2: ASR Only 🎤
**Test Case:** Individual ASR model testing

**Steps:**
1. Navigate to "ASR Only" tab
2. Select each ASR model one at a time
3. Record a test phrase
4. Click "Transcribe"

**Expected Results:**
- [ ] Parakeet TDT 0.6B transcribes correctly
- [ ] Parakeet 1.1B CTC EN-US transcribes correctly
- [ ] Parakeet 1.1B RNNT transcribes correctly
- [ ] Model name displayed in results
- [ ] No API errors

### Tab 3: Translation Only 🌍
**Test Case:** Direct translation testing

**Steps:**
1. Navigate to "Translate Only" tab
2. Enter text: "Hello, how are you?"
3. Select source: English
4. Select target: Spanish
5. Click "Translate"

**Expected Results:**
- [ ] Translation appears: "Hola, ¿cómo estás?"
- [ ] Model name displayed: "Riva Translate 4B"
- [ ] No API errors

**Additional Language Pairs to Test:**
- [ ] English → French
- [ ] English → German
- [ ] Spanish → English
- [ ] French → English

### Tab 4: TTS Only 🔊
**Test Case:** Text-to-speech synthesis

**Steps:**
1. Navigate to "TTS Only" tab
2. Enter text: "This is a test of the text to speech system."
3. Click "Synthesize"

**Expected Results:**
- [ ] Audio player appears
- [ ] Audio plays automatically
- [ ] Speech is clear and natural
- [ ] Model name displayed: "Magpie TTS Multilingual 357M"
- [ ] No API errors

**Test Phrases:**
- [ ] English: "Hello, welcome to the demo."
- [ ] Spanish: "Hola, bienvenido a la demostración."
- [ ] French: "Bonjour, bienvenue à la démo."

---

## Error Handling Testing

### Missing API Key
**Steps:**
1. Unset `INFERENCE_API_KEY`
2. Start app
3. Try any operation

**Expected:**
- [ ] Console shows warning on startup
- [ ] API calls return error: "INFERENCE_API_KEY not set"
- [ ] Error displayed in UI (not console)

### Invalid API Key
**Steps:**
1. Set `INFERENCE_API_KEY=invalid_key`
2. Try any operation

**Expected:**
- [ ] API returns 401 or 403 error
- [ ] Error message displayed to user
- [ ] No application crash

### Network Errors
**Steps:**
1. Disconnect from internet
2. Try any operation

**Expected:**
- [ ] Timeout or connection error
- [ ] User-friendly error message
- [ ] No application crash

### Microphone Denied
**Steps:**
1. Block microphone access in browser
2. Try to record

**Expected:**
- [ ] Alert: "Microphone access denied"
- [ ] No application crash

---

## Performance Testing

### Latency Benchmarks
Record typical response times:

- [ ] ASR (5-10 sec audio): _____ seconds
- [ ] Translation (1 sentence): _____ seconds
- [ ] TTS (1 sentence): _____ seconds
- [ ] Full Pipeline: _____ seconds

**Acceptable ranges:**
- ASR: < 3 seconds
- Translation: < 2 seconds
- TTS: < 3 seconds
- Full Pipeline: < 10 seconds

### Audio Quality
- [ ] Recording quality is clear
- [ ] No audio artifacts or distortion
- [ ] TTS output is natural-sounding
- [ ] Volume levels are appropriate

---

## Demo Rehearsal

### Practice Run #1
**Date:** __________  
**Time:** __________  
**Notes:**

### Practice Run #2
**Date:** __________  
**Time:** __________  
**Notes:**

### Final Rehearsal (Day Before)
**Date:** __________  
**Time:** __________  
**Notes:**

---

## Backup Plans

### If Full Pipeline Fails
- [ ] Fall back to individual tabs
- [ ] Show ASR → Translation separately
- [ ] Show TTS separately

### If Microphone Fails
- [ ] Use pre-recorded audio sample
- [ ] Focus on Translation + TTS tabs
- [ ] Show code walkthrough

### If Network Issues
- [ ] Show pre-captured screenshots
- [ ] Walk through code
- [ ] Show API documentation
- [ ] Reference Datadog probes

---

## Post-Demo Checklist

- [ ] Stop the Flask app
- [ ] Update Linear issue with:
  - [ ] Demo app repo link
  - [ ] Presentation recording link (if available)
  - [ ] Any audience questions/feedback
- [ ] Document any issues encountered
- [ ] Create follow-up tickets if needed

---

## Known Limitations (Be Prepared to Discuss)

1. **WebSocket streaming not supported** - HTTP request/response only
2. **Audio format constraints** - WAV recommended, max ~30 seconds
3. **Translation uses chat-completions** - Not a dedicated translate endpoint
4. **No voice selection for TTS** - Uses default voice
5. **Browser compatibility** - Requires modern browser with getUserMedia API

---

## Emergency Contacts

- Inference Hub team: [TODO - add Slack channel]
- API support: [TODO - add email/Slack]
- Demo backup presenter: [TODO - add name]
