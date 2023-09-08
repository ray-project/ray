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

window.addEventListener("scroll", loadVisibleTermynals);
createTermynals();
loadVisibleTermynals();

// Reintroduce dropdown icons on the sidebar. This is a hack, as we can't
// programmatically figure out which nav items have children anymore.
document.addEventListener("DOMContentLoaded", function() {
    let navItems = document.querySelectorAll(".bd-sidenav li");

    const defaultStyle = {"fontWeight": "bold"}

    const stringList = [
        {"text": "User Guides"},
        {"text": "Examples"},
        // Ray Core
        {"text": "Ray Core"},
        {"text": "Ray Core API"},
        // Ray Cluster
        {"text": "Ray Clusters"},
        {"text": "Deploying on Kubernetes"},
        {"text": "Deploying on VMs"},
        {"text": "Applications Guide"},
        {"text": "Ray Cluster Management API"},
        {"text": "Getting Started with KubeRay"},
        {"text": "KubeRay Ecosystem"},
        {"text": "KubeRay Benchmarks"},
        {"text": "KubeRay Troubleshooting"},
        // Ray AIR
        {"text": "Ray AIR API"},
        // Ray Data
        {"text": "Ray Data"},
        {"text": "Ray Data API"},
        {"text": "Integrations"},
        // Ray Train
        {"text": "Ray Train"},
        {"text": "More Frameworks"},
        {"text": "Advanced Topics"},
        {"text": "Internals"},
        {"text": "Ray Train API"},
        // Ray Tune
        {"text": "Ray Tune"},
        {"text": "Ray Tune Examples"},
        {"text": "Ray Tune API"},
        // Ray Serve
        {"text": "Ray Serve"},
        {"text": "Ray Serve API"},
        {"text": "Production Guide"},
        {"text": "Advanced Guides"},
        {"text": "Deploy Many Models"},
        // Ray RLlib
        {"text": "Ray RLlib"},
        {"text": "Ray RLlib API"},
        // More libraries
        {"text": "More Libraries"},
        {"text": "Ray Workflows (Alpha)"},
        // Monitoring/debugging
        {"text": "Monitoring and Debugging"},
        // References
        {"text": "References"},
        {"text": "Use Cases", "style": {}}, // Don't use default style: https://github.com/ray-project/ray/issues/39172
        // Developer guides
        {"text": "Developer Guides"},
        {"text": "Getting Involved / Contributing"},
    ];

  Array.from(navItems).filter(
    item => stringList.some(({text}) => item.innerText === text) && ! item.classList.contains('current')
  ).forEach((item, i) => {
    if (item.classList.contains('toctree-l1')) {
      const { style } = stringList.find(({text}) => item.innerText == text)

      // Set the style on the menu items
      Object.entries(style ?? defaultStyle).forEach(([key, value]) => {
        item.style[key] = value
      })

    }
    item.innerHTML +=
        `<a href="${item.querySelector("a").getAttribute("href")}" style="display: none">`
        + '<input checked="" class="toctree-checkbox" id="toctree-checkbox-'
        + i + '" name="toctree-checkbox-' + i + '" type="button"></a>'
        + '<label for="toctree-checkbox-' + i + '">' +
        '<i class="fas fa-chevron-down"></i></label>'
  })
});

// Dynamically adjust the height of all panel elements in a gallery to be the same as
// that of the max-height element.
document.addEventListener("DOMContentLoaded", function() {
  let images = document.getElementsByClassName("fixed-height-img");
  let maxHeight = 0;

  for (let i = 0; i < images.length; i++) {
    if (images[i].height > maxHeight) {
      maxHeight = images[i].height;
    }
  }

  for (let i = 0; i < images.length; i++) {
    let margin = Math.floor((maxHeight - images[i].height) / 2);
    images[i].style.cssText = "margin-top: " + margin + "px !important;" +
        "margin-bottom: " + margin + "px !important;"
  }
});

// Remember the scroll position when the page is unloaded.
window.onload = function() {
    let sidebar = document.querySelector("#bd-docs-nav");

    window.onbeforeunload = function() {
        let scroll = sidebar.scrollTop;
        localStorage.setItem("scroll", scroll);
    }

    let storedScrollPosition = localStorage.getItem("scroll");
    if (storedScrollPosition) {
        sidebar.scrollTop = storedScrollPosition;
        localStorage.removeItem("scroll");
    }
};

// When the document is fully loaded
document.addEventListener("DOMContentLoaded", function() {
    // find all the code blocks' copy buttons
    let codeButtons = document.querySelectorAll(".copybtn");
        for (let i = 0; i < codeButtons.length; i++) {
            const button = codeButtons[i];
            // and add a click event listener to each one for Google Analytics.
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
