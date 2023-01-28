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
