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
