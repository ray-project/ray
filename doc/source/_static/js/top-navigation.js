// Remove the black background from the announcement banner. We abuse the
// sphinx-book-theme announcement feature to place a navigation bar on top of the
// documentation. This javascript file replaces the announcement banner with the
// navigation bar.
document.getElementsByClassName("announcement")[0].classList.remove("header-item")

documentation_root = document.getElementsByClassName("navbar-brand")[0].href

// Get the right relative URL for a given path
function getNavURL(url) {
    references = document.getElementsByClassName("reference internal")
    for (let i = 0; i < references.length; i++) {
        if (references[i].href.includes(url)) {
            return references[i].href
        }
    }
}

topNav = document.createElement("div");
topNav.innerHTML += "<a class='active' href='https://ray.io'>Ray</a>"
topNav.innerHTML += "<a href='" + getNavURL("ray-overview/index.html") + "'>Get Started</a>"
topNav.innerHTML += "<a href='https://www.anyscale.com/blog'>Blog</a>"
topNav.innerHTML += "<a href='" + getNavURL("ray-overview/index.html").replace("ray-overview/index.html", "index.html") + "'>Documentation</a>"
topNav.innerHTML += "<a href='" + getNavURL("ray-air/getting-started.html") + "'>Libraries</a>"
topNav.innerHTML += "<a href='" + getNavURL("ray-overview/ray-libraries.html") + "'>Ecosystem</a>"
topNav.innerHTML += "<a href='https://www.ray.io/community'>Community</a>"
document.getElementsByClassName("topnav")[0].append(topNav)

anyscaleButton = document.createElement("button");
anyscaleButton.setAttribute("class", "try-anyscale");
anyscaleButton.innerHTML = '<span>Learn about managed Ray on Anyscale</span><i class="fas fa-chevron-right" aria-hidden="true" title="Hide"></i>'
anyscaleButton.onclick = function () {
    gtag("event", "try_anyscale", {
        "send_to": "UA-110413294-1",
        "event_category": "TryAnyscale",
        "event_label": "TryAnyscale",
        "value": 1,
    });
    window.open('https://www.anyscale.com', '_blank');
};
document.getElementsByClassName("topnav")[0].append(anyscaleButton)
