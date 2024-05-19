let new_termynals = [];

function createTermynals() {
    const containers = document.getElementsByClassName("termynal");
    Array.from(containers).forEach(addTermynal);
}

function addTermynal(container) {
    const t = new Termynal(container, {
        noInit: true,
    });
    new_termynals.push(t);
}

// Initialize Termynals that are visible on the page. Once initialized, remove
// the Termynal from terminals that remain to be loaded.
function loadVisibleTermynals() {
    new_termynals = new_termynals.filter(termynal => {
        if (termynal.container.getBoundingClientRect().top - innerHeight <= 0) {
            termynal.init();
            return false;
        }
        return true;
    });
}

// Store the state of the page in the browser's local storage.
// For now this includes just the sidebar scroll position.
document.addEventListener("DOMContentLoaded", () => {
  const sidebar = document.getElementById("main-sidebar")

  window.addEventListener("beforeunload", () => {
    if (sidebar) {
      localStorage.setItem("scroll", sidebar.scrollTop)
    }
  })

  const storedScrollPosition = localStorage.getItem("scroll")
  if (storedScrollPosition) {
    if (sidebar) {
      sidebar.scrollTop = storedScrollPosition;
    }
    localStorage.removeItem("scroll");
  }

})

// Send GA events any time a code block is copied
document.addEventListener("DOMContentLoaded", function() {
    let codeButtons = document.querySelectorAll(".copybtn");
        for (let i = 0; i < codeButtons.length; i++) {
            const button = codeButtons[i];
            button.addEventListener("click", function() {
                gtag("event", "code_copy_click", {
                     "send_to": "UA-110413294-1",
                     "event_category": "ray_docs_copy_code",
                     "event_label": "URL: " + document.URL
                         + " Button: " + button.getAttribute("data-clipboard-target"),
                     "value": 1,
                });
            });
        }
});

document.addEventListener("DOMContentLoaded", function() {
  let anyscaleButton = document.getElementById("try-anyscale")
  anyscaleButton.onclick = () => {
    gtag("event", "try_anyscale", {
        "send_to": "UA-110413294-1",
        "event_category": "TryAnyscale",
        "event_label": "TryAnyscale",
        "value": 1,
    });
    window.open('https://www.anyscale.com', '_blank');
  }
});

window.addEventListener("scroll", loadVisibleTermynals);
createTermynals();
loadVisibleTermynals();
