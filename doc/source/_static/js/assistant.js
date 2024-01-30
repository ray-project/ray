// Create chat-widget div
var chatWidgetDiv = document.createElement("div");
chatWidgetDiv.className = "chat-widget";
chatWidgetDiv.innerHTML = `
    <button id="openChatBtn">Ask AI</button>
`
document.body.appendChild(chatWidgetDiv);

// Create chat-popup div
var chatPopupDiv = document.createElement("div");
chatPopupDiv.className = "chat-popup";
chatPopupDiv.id = "chatPopup";
chatPopupDiv.innerHTML = `
  <div class="chatHeader">
    <div class="header-wrapper">
      <h3>Ray Docs AI - Ask a question</h3>
    </div>
    <button id="closeChatBtn" class="btn">
      <i class="fas fa-times"></i>
    </button>
  </div>
  <div id="chatContainer" class="chatContentContainer">
    <div id="result">
      Please note that the results of this bot are automated and may be incorrect or contain inappropriate information.
      <div id="anchor"></div>
    </div>
    <div class="input-group">
      <textarea id="searchBar" class="input" rows="3" placeholder="Do not include any personal or confidential information."></textarea>
      <button id="searchBtn" class="btn btn-primary">Ask AI</button>
    </div>
  </div>
  <div class="chatFooter text-right p-2">
    © Copyright 2023, The Ray Team.
  </div>
`
chatPopupDiv.messages = []
document.body.appendChild(chatPopupDiv);

const anchorDiv = document.getElementById('anchor');
const chatContainerDiv = document.getElementById('chatContainer')

const blurDiv = document.createElement('div')
blurDiv.id = "blurDiv"
blurDiv.classList.add("blurDiv-hidden")
document.body.appendChild(blurDiv);

// blur background when chat popup is open
document.getElementById('openChatBtn').addEventListener('click', function() {
  document.getElementById('chatPopup').style.display = 'block';
  blurDiv.classList.remove("blurDiv-hidden");
});

// un-blur background when chat popup is closed
document.getElementById('closeChatBtn').addEventListener('click', function() {
  document.getElementById('chatPopup').style.display = 'none';
  blurDiv.classList.add("blurDiv-hidden");
});

// set code highlighting options
marked.setOptions({
    renderer: new marked.Renderer(),
    highlight: function(code) {
      return hljs.highlight('python', code).value;
    },
});

function highlightCode() {
    document.querySelectorAll('pre code').forEach((block) => {
        hljs.highlightBlock(block);
    });
}

function renderCopyButtons(resultDiv) {
    let preElements = resultDiv.querySelectorAll('pre');
    preElements.forEach((preElement, index) => {
        preElement.style.position = 'relative';

        let uniqueId = `button-id-${index}`;
        preElement.id = uniqueId;

        // Set the proper attributes to the button to make is a sphinx copy button
        let copyButton = document.createElement('button');
        copyButton.className = 'copybtn o-tooltip--left';
        copyButton.setAttribute('data-tooltip', 'Copy');
        copyButton.setAttribute('data-clipboard-target', `#${uniqueId}`);
        copyButton.style.position = 'absolute';
        copyButton.style.top = '10px';
        copyButton.style.right = '10px';
        copyButton.style.opacity = 'inherit';

        let imgElement = document.createElement('img');
        imgElement.src = './_static/copy-button.svg'
        imgElement.alt = 'Copy to clipboard';

        copyButton.appendChild(imgElement);
        preElement.appendChild(copyButton);
    });
}

const searchBar = document.getElementById('searchBar')
const searchBtn = document.getElementById('searchBtn')



function rayAssistant(event) {
    const resultDiv = document.getElementById('result');
    
    const searchTerm = searchBar.value;
    // handle empty input
    if (!searchTerm) {
      return
    }

    if (event.type === 'click' || event.type === 'keydown' && event.key === 'Enter'){
        // for improved UX, we want to equate a carriage return as hitting the send button and prevent default behavior of entering a new line
        if (event.type === 'keydown' && event.key === 'Enter') {
          event.preventDefault();
        }
        // clear search bar value and change placeholder to prompt user to ask follow up question
        searchBar.value = ""
        searchBar.placeholder = "Ask follow up question here"

	// Add query to the list of messages
	chatPopupDiv.messages.push({"role": "user", "content": searchTerm})
        
        let msgBlock = document.createElement('div');
        
        resultDiv.insertBefore(msgBlock, anchorDiv);

        let divider = document.createElement('hr')
        msgBlock.appendChild(divider)

        let msgHeader = document.createElement('div')
        msgHeader.style.fontWeight = 'bold';
        // capitalize first letter of question header
        msgHeader.innerHTML=searchTerm.charAt(0).toUpperCase()+ searchTerm.slice(1)
        msgBlock.appendChild(msgHeader)

        async function readStream() {
            try {
                const response = await fetch('https://ray-assistant-public-bxauk.cld-kvedzwag2qa8i5bj.s.anyscaleuserdata.com/chat', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    },
                    body: JSON.stringify({messages: chatPopupDiv.messages})
                });
                const reader = response.body.getReader();
                let decoder = new TextDecoder('utf-8');
                let msgContent = document.createElement('div')
                msgBlock.appendChild(msgContent)
                msgContent.innerHTML = '';

                let collectChunks = "";

                while (true) {
                    const { done, value } = await reader.read();
                    if (done) {
                        renderCopyButtons(resultDiv);
                        break;
                    }

                    const chunk = decoder.decode(value, {stream: true});
                    collectChunks += chunk;
                    let html = marked.parse(collectChunks);
                    html = DOMPurify.sanitize(html);
                    msgContent.innerHTML = html;
                    highlightCode();
                }
		chatPopupDiv.messages.push({"role": "assistant", "content": collectChunks})
                // we want to autoscroll to the bottom of the chat container after the answer is streamed in
                chatContainerDiv.scrollTo(0, chatContainerDiv.scrollHeight);
            } catch (error) {
                console.error('Fetch API failed:', error);
            }
        }
        readStream().then(
            res => console.log(res)
        );
    }
}

searchBtn.addEventListener('click', rayAssistant);
searchBar.addEventListener('keydown', rayAssistant);
