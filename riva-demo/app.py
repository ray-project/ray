"""
Riva Models Demo App
Showcases 5 Riva-family speech models on inference-api.nvidia.com:
- Parakeet TDT 0.6B (ASR)
- Parakeet 1.1B CTC EN-US (ASR)
- Parakeet 1.1B RNNT Multilingual (ASR)
- Magpie TTS Multilingual 357M (TTS)
- Riva Translate 4B (translation via chat-completions)
"""

import os
import base64
import json
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app)

INFERENCE_API_KEY = os.environ.get('INFERENCE_API_KEY', '')
BASE_URL = 'https://inference-api.nvidia.com'

MODELS = {
    'parakeet_tdt_0_6b': {
        'name': 'Parakeet TDT 0.6B',
        'type': 'asr',
        'endpoint': f'{BASE_URL}/v1/audio/transcriptions',
        'model_id': 'nvidia/parakeet-tdt-0.6b'
    },
    'parakeet_1_1b_ctc': {
        'name': 'Parakeet 1.1B CTC EN-US',
        'type': 'asr',
        'endpoint': f'{BASE_URL}/v1/audio/transcriptions',
        'model_id': 'nvidia/parakeet-1.1b-ctc-en-us'
    },
    'parakeet_1_1b_rnnt': {
        'name': 'Parakeet 1.1B RNNT Multilingual',
        'type': 'asr',
        'endpoint': f'{BASE_URL}/v1/audio/transcriptions',
        'model_id': 'nvidia/parakeet-rnnt-1.1b'
    },
    'magpie_tts': {
        'name': 'Magpie TTS Multilingual 357M',
        'type': 'tts',
        'endpoint': f'{BASE_URL}/v1/audio/speech',
        'model_id': 'nvidia/magpie-tts-multilingual-357m'
    },
    'riva_translate': {
        'name': 'Riva Translate 4B',
        'type': 'translation',
        'endpoint': f'{BASE_URL}/v1/chat/completions',
        'model_id': 'nvidia/riva-translate-4b'
    }
}


def call_asr_model(audio_data_base64, model_key):
    """Call an ASR model with audio data."""
    model = MODELS[model_key]
    
    audio_bytes = base64.b64decode(audio_data_base64)
    
    headers = {
        'Authorization': f'Bearer {INFERENCE_API_KEY}'
    }
    
    files = {
        'file': ('audio.wav', audio_bytes, 'audio/wav')
    }
    
    data = {
        'model': model['model_id']
    }
    
    response = requests.post(
        model['endpoint'],
        headers=headers,
        files=files,
        data=data
    )
    
    response.raise_for_status()
    return response.json()


def call_translation_model(text, source_lang, target_lang):
    """Call Riva Translate 4B via chat-completions API."""
    model = MODELS['riva_translate']
    
    headers = {
        'Authorization': f'Bearer {INFERENCE_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    prompt = f"Translate from {source_lang} to {target_lang}: {text}"
    
    payload = {
        'model': model['model_id'],
        'messages': [
            {
                'role': 'user',
                'content': prompt
            }
        ],
        'temperature': 0.2,
        'max_tokens': 1024
    }
    
    response = requests.post(
        model['endpoint'],
        headers=headers,
        json=payload
    )
    
    response.raise_for_status()
    result = response.json()
    
    translated_text = result['choices'][0]['message']['content']
    return translated_text


def call_tts_model(text, voice='default'):
    """Call Magpie TTS model to synthesize speech."""
    model = MODELS['magpie_tts']
    
    headers = {
        'Authorization': f'Bearer {INFERENCE_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    payload = {
        'model': model['model_id'],
        'input': text,
        'voice': voice
    }
    
    response = requests.post(
        model['endpoint'],
        headers=headers,
        json=payload
    )
    
    response.raise_for_status()
    
    audio_data = response.content
    audio_base64 = base64.b64encode(audio_data).decode('utf-8')
    
    return audio_base64


@app.route('/')
def index():
    """Serve the main demo page."""
    return render_template('index.html', models=MODELS)


@app.route('/api/transcribe', methods=['POST'])
def transcribe():
    """Transcribe audio using selected ASR model."""
    try:
        data = request.get_json()
        audio_data = data.get('audio')
        model_key = data.get('model', 'parakeet_tdt_0_6b')
        
        if not audio_data:
            return jsonify({'error': 'No audio data provided'}), 400
        
        if not INFERENCE_API_KEY:
            return jsonify({'error': 'INFERENCE_API_KEY not set'}), 500
        
        result = call_asr_model(audio_data, model_key)
        
        return jsonify({
            'success': True,
            'text': result.get('text', ''),
            'model': MODELS[model_key]['name']
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/translate', methods=['POST'])
def translate():
    """Translate text using Riva Translate 4B."""
    try:
        data = request.get_json()
        text = data.get('text', '')
        source_lang = data.get('source_lang', 'English')
        target_lang = data.get('target_lang', 'Spanish')
        
        if not text:
            return jsonify({'error': 'No text provided'}), 400
        
        if not INFERENCE_API_KEY:
            return jsonify({'error': 'INFERENCE_API_KEY not set'}), 500
        
        translated = call_translation_model(text, source_lang, target_lang)
        
        return jsonify({
            'success': True,
            'text': translated,
            'model': MODELS['riva_translate']['name']
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/synthesize', methods=['POST'])
def synthesize():
    """Synthesize speech using Magpie TTS."""
    try:
        data = request.get_json()
        text = data.get('text', '')
        voice = data.get('voice', 'default')
        
        if not text:
            return jsonify({'error': 'No text provided'}), 400
        
        if not INFERENCE_API_KEY:
            return jsonify({'error': 'INFERENCE_API_KEY not set'}), 500
        
        audio_base64 = call_tts_model(text, voice)
        
        return jsonify({
            'success': True,
            'audio': audio_base64,
            'model': MODELS['magpie_tts']['name']
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/full-pipeline', methods=['POST'])
def full_pipeline():
    """Run the complete pipeline: ASR → Translation → TTS."""
    try:
        data = request.get_json()
        audio_data = data.get('audio')
        asr_model = data.get('asr_model', 'parakeet_tdt_0_6b')
        source_lang = data.get('source_lang', 'English')
        target_lang = data.get('target_lang', 'Spanish')
        
        if not audio_data:
            return jsonify({'error': 'No audio data provided'}), 400
        
        if not INFERENCE_API_KEY:
            return jsonify({'error': 'INFERENCE_API_KEY not set'}), 500
        
        asr_result = call_asr_model(audio_data, asr_model)
        transcribed_text = asr_result.get('text', '')
        
        translated_text = call_translation_model(transcribed_text, source_lang, target_lang)
        
        audio_base64 = call_tts_model(translated_text)
        
        return jsonify({
            'success': True,
            'transcribed': transcribed_text,
            'translated': translated_text,
            'audio': audio_base64,
            'models_used': {
                'asr': MODELS[asr_model]['name'],
                'translation': MODELS['riva_translate']['name'],
                'tts': MODELS['magpie_tts']['name']
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({
        'status': 'ok',
        'api_key_configured': bool(INFERENCE_API_KEY),
        'models': list(MODELS.keys())
    })


if __name__ == '__main__':
    if not INFERENCE_API_KEY:
        print("⚠️  WARNING: INFERENCE_API_KEY environment variable not set!")
        print("Set it with: export INFERENCE_API_KEY=your_key_here")
    
    print("🚀 Riva Models Demo starting...")
    print(f"📡 Using inference endpoint: {BASE_URL}")
    print(f"🎯 Models configured: {len(MODELS)}")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
