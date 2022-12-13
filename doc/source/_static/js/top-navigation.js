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

is_get_started = window.location.href.endsWith("ray-overview/index.html")
is_use_cases = window.location.href.endsWith("ray-overview/use-cases.html")
is_libraries = window.location.href.includes("/ray-core/") ||
    window.location.href.includes("/ray-air/") ||
    window.location.href.includes("/data/") ||
    window.location.href.includes("/train/") ||
    window.location.href.includes("/tune/") ||
    window.location.href.includes("/serve/") ||
    window.location.href.includes("/rllib/")
is_ecosystem = window.location.href.endsWith("ray-overview/ray-libraries.html")
is_documentation = !(is_get_started || is_use_cases || is_libraries || is_ecosystem)

downCaret = '<span class="down-caret"><svg width="12" height="7" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M11 1 6 6 1 1" stroke="#2A2A2A" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"></path></svg></span>'
rayLogoSvg = "<svg viewBox='0 0 110 44' fill='none' xmlns='http://www.w3.org/2000/svg'><path d='M66.225 32.133h2.716l-4.89-7.102c3.008-.99 4.677-3.299 4.677-6.753 0-4.443-3.008-6.985-8.208-6.985h-7.936v20.84h2.387v-6.578h5.53c.485 0 .95-.04 1.397-.059l4.327 6.637ZM54.97 23.245v-9.663h5.53c3.783 0 5.918 1.61 5.918 4.753 0 3.24-2.135 4.91-5.918 4.91h-5.53Zm32.617 3.609 2.329 5.258h2.58l-9.313-20.84h-2.465l-9.352 20.84h2.522l2.329-5.258h11.37Zm-1.009-2.29h-9.352l4.657-10.575 4.695 10.575Zm15.504.407L110 11.274h-2.445L100.88 22.14l-6.752-10.867h-2.465l8.072 13.777v7.063h2.348v-7.14Z' fill='#2A2A2A'></path><path d='M15.989 20.258a6.013 6.013 0 0 1 1.552-2.736 5.88 5.88 0 0 1 4.172-1.727c1.63 0 3.104.66 4.172 1.727a6.011 6.011 0 0 1 1.552 2.736h4.346a5.746 5.746 0 0 1 .66-1.592l-7.703-7.703a5.954 5.954 0 0 1-3.027.835 5.88 5.88 0 0 1-4.172-1.727 5.88 5.88 0 0 1-1.727-4.172c0-1.63.66-3.105 1.727-4.172A5.88 5.88 0 0 1 21.713 0c1.63 0 3.104.66 4.172 1.727a5.88 5.88 0 0 1 1.727 4.172 5.9 5.9 0 0 1-.835 3.027l7.703 7.703a5.954 5.954 0 0 1 3.028-.834c1.63 0 3.104.66 4.171 1.727a5.88 5.88 0 0 1 1.727 4.171 5.88 5.88 0 0 1-1.727 4.172 5.88 5.88 0 0 1-4.172 1.727 5.9 5.9 0 0 1-3.026-.834l-7.704 7.723c.524.892.835 1.92.835 3.026a5.88 5.88 0 0 1-1.727 4.172 5.88 5.88 0 0 1-4.172 1.727 5.88 5.88 0 0 1-4.172-1.727 5.88 5.88 0 0 1-1.727-4.172c0-1.63.66-3.104 1.727-4.171a5.88 5.88 0 0 1 4.172-1.727 5.9 5.9 0 0 1 3.027.834l7.703-7.703a5.746 5.746 0 0 1-.66-1.591h-4.346a6.011 6.011 0 0 1-1.552 2.736 5.88 5.88 0 0 1-4.172 1.727 5.88 5.88 0 0 1-4.172-1.727 6.013 6.013 0 0 1-1.552-2.736h-4.347a6.013 6.013 0 0 1-1.552 2.736 5.88 5.88 0 0 1-4.172 1.727 5.88 5.88 0 0 1-4.172-1.727A5.817 5.817 0 0 1 0 21.713c0-1.63.66-3.105 1.727-4.172a5.88 5.88 0 0 1 4.172-1.727c1.63 0 3.104.66 4.172 1.727a6.014 6.014 0 0 1 1.552 2.736h4.366v-.02Zm3.59 19.384c.543.543 1.3.892 2.134.892.834 0 1.59-.33 2.134-.892.543-.543.893-1.3.893-2.135 0-.834-.33-1.59-.893-2.134a3.022 3.022 0 0 0-2.134-.892c-.835 0-1.591.33-2.135.892a3.022 3.022 0 0 0-.892 2.135c0 .834.33 1.59.892 2.134Zm20.063-15.795c.543-.543.892-1.3.892-2.134 0-.835-.33-1.591-.892-2.135a3.022 3.022 0 0 0-2.135-.892c-.834 0-1.59.33-2.134.892a3.022 3.022 0 0 0-.892 2.135c0 .834.33 1.59.892 2.134.543.543 1.3.893 2.135.893a3.106 3.106 0 0 0 2.134-.893ZM23.847 3.764a3.022 3.022 0 0 0-2.134-.892c-.835 0-1.591.33-2.135.892a3.022 3.022 0 0 0-.892 2.135c0 .834.33 1.59.892 2.134.544.543 1.3.893 2.135.893.834 0 1.59-.33 2.134-.893.543-.543.893-1.3.893-2.134a3.106 3.106 0 0 0-.893-2.135ZM3.764 19.578a3.022 3.022 0 0 0-.892 2.135c0 .834.33 1.59.892 2.134.544.543 1.3.893 2.135.893.834 0 1.59-.33 2.134-.893.543-.563.893-1.3.893-2.134 0-.835-.33-1.591-.893-2.135-.563-.543-1.3-.892-2.134-.892-.835 0-1.591.33-2.135.892Zm15.814 0a3.022 3.022 0 0 0-.892 2.135c0 .834.33 1.59.892 2.134.544.543 1.3.893 2.135.893.834 0 1.59-.33 2.134-.893.543-.543.893-1.3.893-2.134 0-.835-.33-1.591-.893-2.135a3.022 3.022 0 0 0-2.134-.892c-.835 0-1.591.33-2.135.892Z' fill='#028CF0'></path></svg>"

topNavContent = document.createElement("div");
topNavContent.setAttribute("class", "top-nav-content")

// The left part that contains links and menus
topNavContentLeft = document.createElement("div");
topNavContentLeft.setAttribute("class", "left")

//-- The Ray link
linkRay = document.createElement("a")
linkRay.setAttribute("href", "https://ray.io")
linkRay.setAttribute("class", "ray-logo")
linkRay.innerHTML += rayLogoSvg;
topNavContentLeft.append(linkRay)

//-- The Get started link
getStartedLink = document.createElement("a")
getStartedLink.innerText = "Get started"
getStartedLink.setAttribute("href", getNavURL("ray-overview/index.html"))
if (is_get_started) {
    getStartedLink.style.borderBottom = "2px solid #007bff"
}
topNavContentLeft.append(getStartedLink)

//-- The Blog link
// blogLink = document.createElement("a")
// blogLink.innerText = "Blog"
// blogLink.setAttribute("href", "https://www.anyscale.com/blog")
// topNavContentLeft.append(blogLink)

//-- The Use Cases link
useCasesLink = document.createElement("a")
useCasesLink.innerText = "Use cases"
useCasesLink.setAttribute("href", getNavURL("ray-overview/use-cases.html"))
if (is_use_cases) {
    useCasesLink.style.borderBottom = "2px solid #007bff"
}
topNavContentLeft.append(useCasesLink)

//-- The Libraries menu
librariesMenu = document.createElement("div")
librariesMenu.setAttribute("class", "menu")
librariesMenu.innerHTML = "<a href='#'>Libraries" + downCaret + "</a>"
librariesList = document.createElement("ul")
librariesList.innerHTML += "<li><a href='" + getNavURL("ray-core/walkthrough.html") + "'><span class='primary'>Ray Core</span><span class='secondary'>Scale general Python applications</span></a></li>"
librariesList.innerHTML += "<li><a href='" + getNavURL("ray-air/getting-started.html") + "'><span class='primary'>Ray AIR</span><span class='secondary'>Scale AI applications</span></a></li>"
librariesList.innerHTML += "<li><a href='" + getNavURL("data/dataset.html") + "'><span class='primary'>Ray Datasets</span><span class='secondary'>Scale data ingest and preprocessing</span></a></li>"
librariesList.innerHTML += "<li><a href='" + getNavURL("train/train.html") + "'><span class='primary'>Ray Train</span><span class='secondary'>Scale machine learning training</span></a></li>"
librariesList.innerHTML += "<li><a href='" + getNavURL("tune/index.html") + "'><span class='primary'>Ray Tune</span><span class='secondary'>Scale hyperparameter tuning</span></a></li>"
librariesList.innerHTML += "<li><a href='" + getNavURL("serve/index.html") + "'><span class='primary'>Ray Serve</span><span class='secondary'>Scale model serving</span></a></li>"
librariesList.innerHTML += "<li><a href='" + getNavURL("rllib/index.html") + "'><span class='primary'>Ray RLlib</span><span class='secondary'>Scale reinforcement learning</span></a></li>"
librariesMenu.append(librariesList)
if (is_libraries) {
    librariesMenu.style.borderBottom = "2px solid #007bff"
}
topNavContentLeft.append(librariesMenu)

//-- The Documentation link
documentationLink = document.createElement("a")
documentationLink.innerText = "Docs"
documentationLink.setAttribute("href", getNavURL("ray-overview/index.html").replace("ray-overview/index.html", "index.html"))
if (is_documentation) {
    documentationLink.style.borderBottom = "2px solid #007bff"
}
topNavContentLeft.append(documentationLink)

//-- The Learn menu
learnMenu = document.createElement("div")
learnMenu.setAttribute("class", "menu")
learnMenu.innerHTML = "<a href='#'>Resources" + downCaret + "</a>"
learnList = document.createElement("ul")
learnList.innerHTML += "<li><a href='https://discuss.ray.io/'><span class='primary'>Discussion Forum</span><span class='secondary'>Get your Ray questions answered</span></a></li>"
learnList.innerHTML += "<li><a href='https://www.anyscale.com/events?type=anyscale-academy'><span class='primary'>Training</span><span class='secondary'>Hands-on learning</span></a></li>"
learnList.innerHTML += "<li><a href='https://www.anyscale.com/events'><span class='primary'>Events</span><span class='secondary'>Webinars, meetups, office hours</span></a></li>"
learnList.innerHTML += "<li><a href='https://www.anyscale.com/user-stories'><span class='primary'>Success Stories</span><span class='secondary'>Real-world workload examples</span></a></li>"
learnList.innerHTML += "<li><a href='" + getNavURL("/ray-overview/ray-libraries.html") + "'><span class='primary'>Ecosystem</span><span class='secondary'>Libraries integrated with Ray</span></a></li>"
learnList.innerHTML += "<li><a href='https://www.ray.io/community'><span class='primary'>Community</span><span class='secondary'>Connect with us</span></a></li>"
learnMenu.append(learnList)
topNavContentLeft.append(learnMenu)

//-- The Ecosystem link
// ecosystemLink = document.createElement("a")
// ecosystemLink.innerText = "Ecosystem"
// ecosystemLink.setAttribute("href", getNavURL("/ray-overview/ray-libraries.html"))
// if (is_ecosystem) {
//     ecosystemLink.style.borderBottom = "2px solid #007bff"
// }
// topNavContentLeft.append(ecosystemLink)

//-- The Community link
// communityLink = document.createElement("a")
// communityLink.innerText = "Community"
// communityLink.setAttribute("href", "https://www.ray.io/community")
// topNavContentLeft.append(communityLink)

topNavContent.append(topNavContentLeft)

// The right part that contains the Anyscale trial button
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