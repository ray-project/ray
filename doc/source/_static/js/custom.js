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


let firstLink = document.getElementsByClassName("caption")[0];
firstLink.classList.add("toctree-l1", "current");
firstLink.style.textTransform = "none";
firstLink.style.fontWeight = "normal";
firstLink.innerText = "";

let home = document.createElement("a");
home.classList.add("reference", "internal");

const version = window.location.href.split("/")[4];
const res = (version === "latest" || version === "master") ? version : "latest";

home.href = "https://docs.ray.io/en/" + res + "/index.html";
home.textContent = "Ray Docs Home";

home.style = firstLink.style;
home.style.color = "#5a5a5a";
firstLink.appendChild(home);
