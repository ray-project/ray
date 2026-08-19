/* Section-scoped sidebar loader (Pattern B).
 *
 * Fetches this section's shared nav fragment (_static/<section>-nav.html), injects it
 * into the #section-nav-mount container, highlights the current page, collapses
 * non-current sections, and resolves the fragment's root-relative links against this
 * page. The fragment is fetched once per section and reused from the browser cache
 * across pages. Which fragment to load is set server-side per page; see
 * _ext/api_sidebar.py and _templates/section-sidebar.html.
 *
 * The mount arrives with a server-rendered fallback nav (the section's top-level
 * pages) already in it, so no-JS readers get real navigation. This script only ever
 * *upgrades* that: on a failed fetch it leaves the fallback alone rather than
 * replacing it with an error message.
 */
(function () {
  "use strict";

  function init() {
    var mount = document.getElementById("section-nav-mount");
    if (!mount) return;

    var navUrl = mount.getAttribute("data-section-nav-url");
    var pagename = mount.getAttribute("data-pagename") || "";
    // Path from this page back to the doc root (e.g. "../../"), derived from the
    // fetch URL so we don't depend on other globals.
    var root = navUrl.replace(/_static\/[^/]+\.html(\?.*)?$/, "");

    fetch(navUrl)
      .then(function (resp) {
        if (!resp.ok) throw new Error("HTTP " + resp.status);
        return resp.text();
      })
      .then(function (html) {
        mount.innerHTML = html;
        mount.setAttribute("aria-busy", "false");

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
        // Keep the server-rendered fallback nav; a shallower sidebar beats none.
        mount.setAttribute("aria-busy", "false");
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
