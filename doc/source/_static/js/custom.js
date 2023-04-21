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
    for (let i = 0; i < navItems.length; i++) {
        let navItem = navItems[i];
        const stringList = [
            "User Guide", "Examples",
            "Ray Core", "Ray Core API",
            "Ray Clusters", "Deploying on Kubernetes", "Deploying on VMs",
            "Applications Guide", "Ray Cluster Management API",
            "Ray AI Runtime (AIR)", "Ray AIR API",
            "Ray Data", "Ray Data API", "Integrations",
            "Ray Train", "Ray Train API",
            "Ray Tune", "Ray Tune Examples", "Ray Tune API",
            "Ray Serve", "Ray Serve API",
            "Ray RLlib", "Ray RLlib API",
            "More Libraries", "Ray Workflows (Alpha)",
            "Monitoring and Debugging",
            "References",
            "Developer Guides", "Getting Involved / Contributing",
        ];

        const containsString = stringList.some(str => navItem.innerText ===str);

        if (containsString && ! navItem.classList.contains('current')) {
            if (navItem.classList.contains('toctree-l1')) {
                navItem.style.fontWeight = "bold";
            }
            const href = navItem.querySelector("a").getAttribute("href");
            navItem.innerHTML +=
                '<a href="'+ href +'" style="display: none">'
                + '<input checked="" class="toctree-checkbox" id="toctree-checkbox-'
                + i + '" name="toctree-checkbox-' + i + '" type="button"></a>'
                + '<label for="toctree-checkbox-' + i + '">' +
                '<i class="fas fa-chevron-down"></i></label>'
        }
    }
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
