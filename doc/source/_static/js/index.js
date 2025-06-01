function animateTabs() {
  const tabs = Array.from(document.getElementById('v-pills-tab').children);
  const contentTabs = Array.from(
    document.getElementById('v-pills-tabContent').children,
  );

  tabs.forEach((item, index) => {
    item.onclick = () => {
      tabs.forEach((tab, i) => {
        if (i === index) {
          item.classList.add('active');
        } else {
          tab.classList.remove('active');
        }
      });
      contentTabs.forEach((tab, i) => {
        if (i === index) {
          tab.classList.add('active', 'show');
        } else {
          tab.classList.remove('active', 'show');
        }
      });
    };
  });
}

function updateHighlight() {
  const {theme} = document.documentElement.dataset;
  ['dark', 'light'].forEach((title) => {
    const stylesheet = document.querySelector(`link[title="${title}"]`);
    if (title === theme) {
      stylesheet.removeAttribute('disabled');
    } else {
      stylesheet.setAttribute('disabled', 'disabled');
    }
  });
}

function setHighlightListener() {
  const observer = new MutationObserver((mutations) => updateHighlight());
  observer.observe(document.documentElement, {
    attributes: true,
    attributeFilter: ['data-theme'],
  });
}

document.addEventListener('DOMContentLoaded', animateTabs);
document.addEventListener('DOMContentLoaded', () => {
  hljs.highlightAll();
  updateHighlight();
});
document.addEventListener('DOMContentLoaded', setHighlightListener);
