#!/usr/bin/env python3
"""
Quick test script to verify Riva models are accessible.
Requires INFERENCE_API_KEY environment variable.
"""

import os
import sys
import requests

INFERENCE_API_KEY = os.environ.get('INFERENCE_API_KEY', '')
BASE_URL = 'https://inference-api.nvidia.com'

def test_health():
    """Test that API key is set."""
    if not INFERENCE_API_KEY:
        print("❌ INFERENCE_API_KEY not set")
        print("Set it with: export INFERENCE_API_KEY=your_key_here")
        return False
    print("✅ API key is configured")
    return True

def test_translation():
    """Test Riva Translate 4B."""
    print("\n🧪 Testing Riva Translate 4B...")
    
    headers = {
        'Authorization': f'Bearer {INFERENCE_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    payload = {
        'model': 'nvidia/riva-translate-4b',
        'messages': [
            {'role': 'user', 'content': 'Translate from English to Spanish: Hello, how are you?'}
        ],
        'temperature': 0.2,
        'max_tokens': 1024
    }
    
    try:
        response = requests.post(
            f'{BASE_URL}/v1/chat/completions',
            headers=headers,
            json=payload,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            translation = result['choices'][0]['message']['content']
            print(f"✅ Translation works: '{translation}'")
            return True
        else:
            print(f"❌ Translation failed: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Translation error: {e}")
        return False

def test_tts():
    """Test Magpie TTS."""
    print("\n🧪 Testing Magpie TTS Multilingual 357M...")
    
    headers = {
        'Authorization': f'Bearer {INFERENCE_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    payload = {
        'model': 'nvidia/magpie-tts-multilingual-357m',
        'input': 'Hello, this is a test.',
        'voice': 'default'
    }
    
    try:
        response = requests.post(
            f'{BASE_URL}/v1/audio/speech',
            headers=headers,
            json=payload,
            timeout=30
        )
        
        if response.status_code == 200:
            audio_size = len(response.content)
            print(f"✅ TTS works: Generated {audio_size} bytes of audio")
            return True
        else:
            print(f"❌ TTS failed: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ TTS error: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Riva Models Test Suite\n")
    print(f"Testing endpoint: {BASE_URL}\n")
    
    results = []
    
    if not test_health():
        sys.exit(1)
    
    results.append(test_translation())
    results.append(test_tts())
    
    print("\n" + "="*50)
    if all(results):
        print("✅ All tests passed! Ready for demo.")
        print("\nStart the demo app with:")
        print("  python app.py")
    else:
        print("❌ Some tests failed. Check your API key and network connection.")
        sys.exit(1)

if __name__ == '__main__':
    main()
