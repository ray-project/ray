// Remove the black background from the announcement banner. We abuse the
// sphinx-book-theme announcement feature to place a navigation bar on top of the
// documentation. This javascript file replaces the announcement banner with the
// navigation bar.
document.getElementsByClassName("announcement")[0].classList.remove("header-item")

// Get the right relative URL for a given path
function getNavURL(url) {
    references = document.getElementsByClassName("reference internal")
    for (let i = 0; i < references.length; i++) {
        if (references[i].href.includes(url)) {
            return references[i].href
        }
    }
}

topNavContent = document.createElement("div");
topNavContent.setAttribute("class", "top-nav-content")

topNavContentLeft = document.createElement("div");
topNavContentLeft.setAttribute("class", "left")

topNavContentLeft.innerHTML += "<a href='https://ray.io'>Ray</a>"
topNavContentLeft.innerHTML += "<a href='" + getNavURL("ray-overview/index.html") + "'>Get Started</a>"
topNavContentLeft.innerHTML += "<a href='https://www.anyscale.com/blog'>Blog</a>"
topNavContentLeft.innerHTML += "<a class='active' href='" + getNavURL("ray-overview/index.html").replace("ray-overview/index.html", "index.html") + "'>Documentation</a>"
topNavContentLeft.innerHTML += "<a href='" + getNavURL("ray-air/getting-started.html") + "'>Libraries</a>"
topNavContentLeft.innerHTML += "<a href='" + getNavURL("ray-overview/ray-libraries.html") + "'>Ecosystem</a>"
topNavContentLeft.innerHTML += "<a href='https://www.ray.io/community'>Community</a>"
topNavContent.append(topNavContentLeft)

anyscaleButton = document.createElement("button");
anyscaleButton.setAttribute("class", "try-anyscale");
anyscaleButton.innerHTML = '<span>Managed Ray on Anyscale</span><i class="fas fa-chevron-right" aria-hidden="true" title="Hide"></i>'
anyscaleButton.onclick = function () {
    gtag("event", "try_anyscale", {
        "send_to": "UA-110413294-1",
        "event_category": "TryAnyscale",
        "event_label": "TryAnyscale",
        "value": 1,
    });
    window.open('https://www.anyscale.com', '_blank');
};

topNavContent.append(anyscaleButton)

document.getElementsByClassName("topnav")[0].append(topNavContent)