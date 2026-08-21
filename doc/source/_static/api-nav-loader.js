/* APIs-tab sidebar loader (Pattern B).
 *
 * Fetches the single shared API-nav fragment (_static/api-nav.html), injects it into
 * the #api-nav-mount container, highlights the current page, collapses non-current
 * sections, and resolves the fragment's root-relative links against this page.
 * The fragment is fetched once and reused from the browser cache across pages.
 */
(function () {
  "use strict";

  function init() {
    var mount = document.getElementById("api-nav-mount");
    if (!mount) return;

    var navUrl = mount.getAttribute("data-api-nav-url");
    var pagename = mount.getAttribute("data-pagename") || "";
    // Path from this page back to the doc root (e.g. "../../"), derived from the
    // fetch URL so we don't depend on other globals.
    var root = navUrl.replace(/_static\/api-nav\.html(\?.*)?$/, "");

    fetch(navUrl)
      .then(function (resp) {
        if (!resp.ok) throw new Error("HTTP " + resp.status);
        return resp.text();
      })
      .then(function (html) {
        mount.innerHTML = html;

        var links = mount.querySelectorAll("a[href]");

        // 1) Find + mark the current page (fragment hrefs are root-relative).
        var currentHref = pagename + ".html";
        var currentLink = null;
        links.forEach(function (a) {
          if (a.getAttribute("href") === currentHref) currentLink = a;
        });

        // 2) Collapse every section, then re-open only the current page's ancestors.
        mount.querySelectorAll("details").forEach(function (d) {
          d.removeAttribute("open");
        });
        if (currentLink) {
          currentLink.classList.add("current");
          var li = currentLink.closest("li");
          if (li) li.classList.add("current", "active");
          var el = currentLink.parentElement;
          while (el && el !== mount) {
            if (el.tagName === "DETAILS") el.setAttribute("open", "");
            el = el.parentElement;
          }
          if (li) {
            var own = li.querySelector(":scope > details");
            if (own) own.setAttribute("open", "");
          }
          if (li && li.scrollIntoView) {
            li.scrollIntoView({ block: "nearest" });
          }
        }

        // 3) Resolve root-relative hrefs against this page.
        links.forEach(function (a) {
          var h = a.getAttribute("href");
          if (h && !/^(https?:|\/|#|mailto:)/.test(h)) {
            a.setAttribute("href", root + h);
          }
        });
      })
      .catch(function () {
        mount.innerHTML =
          '<p class="api-nav-status">API navigation failed to load.</p>';
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
