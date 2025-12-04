const ASSISTANT_ID = '1003';

const script = document.createElement('script');
script.type = 'module';
script.id = 'runllm-widget-script';
script.src = 'https://widget.runllm.com';
script.setAttribute('version', 'stable');
script.setAttribute('crossorigin', 'anonymous');
script.setAttribute('runllm-keyboard-shortcut', 'Mod+j');
script.setAttribute('runllm-name', 'Ray RunLLM Bot');
script.setAttribute('runllm-position', 'BOTTOM_RIGHT');
script.setAttribute('runllm-assistant-id', ASSISTANT_ID);
script.async = true;
document.head.appendChild(script);
