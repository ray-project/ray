let panels = [];
let tags = [];


window.addEventListener('load', () => {

    const pageUrl = window.location.href;
    const isGallery = pageUrl.endsWith("ray-overview/examples.html");

    if (isGallery) {
        let navBar = document.getElementById("site-navigation");

        // Recursively remove all children of the navbar.
        while (navBar.firstChild) {
          navBar.firstChild.remove();
        }

        let tagString = "<div>\n" +
            "<h5>Filter examples by</h5>\n" +
            "<h6>Use cases</h6>\n" +
            "<div type=\"button\" id=\"llm\" class=\"tag btn btn-outline-primary\">Large Language Models</div>\n" +
            "<div type=\"button\" id=\"cv\" class=\"tag btn btn-outline-primary\">Computer Vision</div>\n" +
            "<div type=\"button\" id=\"ts\" class=\"tag btn btn-outline-primary\">Time-series</div>\n" +
            "<div type=\"button\" id=\"nlp\" class=\"tag btn btn-outline-primary\">Natural Language Processing</div>\n" +
            "<div type=\"button\" id=\"rl\" class=\"tag btn btn-outline-primary\">Reinforcement Learning</div>\n" +
            "<h6>ML Workloads</h6>\n" +
            "<div type=\"button\" id=\"data-processing\" class=\"tag btn btn-outline-primary\">Data Processing</div>\n" +
            "<div type=\"button\" id=\"training\" class=\"tag btn btn-outline-primary\">Model Training</div>\n" +
            "<div type=\"button\" id=\"tuning\" class=\"tag btn btn-outline-primary\">Hyperparameter Tuning</div>\n" +
            "<div type=\"button\" id=\"inference\" class=\"tag btn btn-outline-primary\">Batch Inference</div>\n" +
            "<div type=\"button\" id=\"serving\" class=\"tag btn btn-outline-primary\">Model Serving</div>\n" +
            "<h6>MLOps</h6>\n" +
            "<div type=\"button\" id=\"tracking\" class=\"tag btn btn-outline-primary\">Experiment Tracking</div>\n" +
            "<div type=\"button\" id=\"monitoring\" class=\"tag btn btn-outline-primary\">Monitoring</div>\n" +
            "<h6>ML Frameworks</h6>\n" +
            "<div type=\"button\" id=\"pytorch\" class=\"tag btn btn-outline-primary\">PyTorch</div>\n" +
            "<div type=\"button\" id=\"tensorflow\" class=\"tag btn btn-outline-primary\">TensorFlow/Keras</div>\n" +
            "</div>";

        const tempDiv = document.createElement('div');
        tempDiv.innerHTML = tagString;
        const newNav = tempDiv.firstChild;
        navBar.appendChild(newNav);
    }

    tags = document.querySelectorAll('.tag');
    panels = document.querySelectorAll('.gallery-item');

    tags.forEach(tag => {

        tag.addEventListener('click', () => {
            const tagName = tag.id;

            // Toggle "tag" buttons on click.
            if (tag.classList.contains('btn-primary')) {
                // deactivate filter button
                tag.classList.replace('btn-primary', 'btn-outline-primary');
                panels.forEach(element => {
                    const activeTags = document.querySelectorAll('.tag.btn-primary');
                    let activeIds = Array.from(activeTags).map(function(element) {
                      return element.id;
                    });
                    let panelHasActiveTag = false;
                    if (! activeIds.length) {
                        panelHasActiveTag = true;
                    }
                    activeIds.forEach(id => {
                        if (element.classList.contains(id)) {
                            panelHasActiveTag = true;
                        }
                    });
                    // every panel that does not have the tag
                    // but still has an active tag should be shown again
                    if (! element.classList.contains(tagName) && panelHasActiveTag) {
                        element.style.cssText = 'display: flex !important'
                    }
                });
            } else {
                // activate filter button
                tag.classList.replace('btn-outline-primary', 'btn-primary');
                panels.forEach(element => {
                    // every panel that does not have the tag should be hidden
                    if (! element.classList.contains(tagName)) {
                        element.style.cssText = 'display: none !important'
                    }
                });
            }
        });
    });

    const searchInput = document.getElementById("searchInput");
    const filterButton = document.getElementById("filterButton");

    if (filterButton) {
        filterButton.addEventListener('click', function (event) {
            const query = searchInput.value.toLowerCase();
            const gallery = document.querySelectorAll(".sd-row.docutils")[0];
            const items = gallery.querySelectorAll(".sd-col");

            items.forEach(item => {
                let context = item.textContent + item.classList.toString();
                context = context.toLowerCase();
                if (context.includes(query)) {
                    item.style.cssText = 'display: flex !important';
                } else {
                    item.style.cssText = 'display: none !important'
                }
            });
        });
    }

    if (searchInput) {
        searchInput.addEventListener("keyup", function (event) {
            event.preventDefault();
            if (event.keyCode === 13) {
                filterButton.click();
            }
        });
    }

});


