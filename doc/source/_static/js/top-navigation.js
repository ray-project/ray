// Remove the black background from the announcement banner
document.getElementsByClassName("announcement")[0].classList.remove("header-item")

documentation_root = document.getElementsByClassName("navbar-brand")[0].href

topNav = document.createElement("div");
topNav.innerHTML += "<a class='active' href='https://ray.io'>Ray</a>"
topNav.innerHTML += "<a href='" + documentation_root.replace("index.html", "ray-overview/index.html") + "'>Get Started</a>"
topNav.innerHTML += "<a href='https://www.anyscale.com/blog'>Blog</a>"
topNav.innerHTML += "<a href='" + documentation_root + "'>Documentation</a>"
topNav.innerHTML += "<a href='" + documentation_root.replace("index.html", "ray-air/getting-started.html") + "'>Libraries</a>"
topNav.innerHTML += "<a href='" + documentation_root.replace("index.html", "ray-overview/ray-libraries.html") + "'>Ecosystem</a>"
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