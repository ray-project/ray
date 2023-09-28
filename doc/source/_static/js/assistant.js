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
    <div class="chatHeader bg-light p-2 d-flex justify-content-between align-items-center">
        <div style="width: 30px;"></div>
        <div class="text-center w-100">
            <b>Ray Docs AI - Ask a question</b>
        </div>
        <button id="closeChatBtn" class="btn">
            <i class="fas fa-times"></i>
        </button>
    </div>
    <div class="chatContentContainer">
        <div class="input-group">
            <textarea id="searchBar" class="input" rows="3" placeholder="Do not include any personal or confidential information."></textarea>
            <div class="input-group-append">
                <button id="searchBtn" class="btn btn-primary">Ask AI</button>
            </div>
        </div>
        <div id="result"></div>
    </div>
    <div class="chatFooter text-right p-2">
        © Copyright 2023, The Ray Team.
    </div>
`
document.body.appendChild(chatPopupDiv);

// blur background when chat popup is open
document.getElementById('openChatBtn').addEventListener('click', function() {
    document.getElementById('chatPopup').style.display = 'block';
    document.querySelector('.container-xl').classList.add('blurred');
});

// un-blur background when chat popup is closed
document.getElementById('closeChatBtn').addEventListener('click', function() {
    document.getElementById('chatPopup').style.display = 'none';
    document.querySelector('.container-xl').classList.remove('blurred');
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

    if (event.type === 'click' || event.type === 'keydown' && event.key === 'Enter'){
        resultDiv.textContent = '';
        resultDiv.textContent = `
        Please note that the results of this bot are automated &
        may be incorrect or contain inappropriate information.`;

        async function readStream() {
            try {
                const response = await fetch('https://ray-assistant-public-bxauk.cld-kvedzwag2qa8i5bj.s.anyscaleuserdata.com/stream', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    },
                    body: JSON.stringify({query: searchTerm})
                });
                const reader = response.body.getReader();
                let decoder = new TextDecoder('utf-8');
                resultDiv.innerHTML = '';
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
                    resultDiv.innerHTML = html;
                    highlightCode();
                }
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
