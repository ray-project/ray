let panels = [];
let tags = [];

window.addEventListener('load', () => {

    $("div.tag").css("margin-bottom", "10px");

    // Fetch the mapping of panel IDs to their respective tags from a static
    // JSON file.
    fetch('/_static/tag-mapping.json')
      .then(response => response.json())
      .then(panelTagMap => {

          for (const panelId in panelTagMap) {
              let element = document.getElementsByClassName(panelId)[0];

              // For each panel, attach data tags to the 4-th parent of the panel,
              // which is the "div" element that we can later toggle.
              // Sphinx Panels is too inflexible to allow us to attach data tags
              // directly to the container.
              for (let i = 0; i < 4; i++) {
                  if (element.parentNode) {
                      element = element.parentElement;
                      element.setAttribute('data-tags', panelTagMap[panelId]);
                  }
                  else {
                      console.log(panelId + ' has no parent element,' +
                          'please check if the panel has been tagged correctly.');
                  }
              }
          }

          const allButton = document.getElementById('allButton')
          panels = document.querySelectorAll('.d-flex.docutils');
          tags = document.querySelectorAll('.tag');
          tags = Array.from(tags).filter(function(node) {
            return node.id !== "allButton";
          });

          // If the "all" button is clicked
          allButton.addEventListener('click', () => {

              // Activate the "all" button
              allButton.classList.replace('btn-outline-primary', 'btn-primary');

              // Deactivate all other buttons
              for (const tag of tags) {
                  tag.classList.replace('btn-primary', 'btn-outline-primary');
              }
              // And show all panels
              for (const panel of panels) {
                  panel.style.cssText = 'display: flex !important';
              }
          });

          tags.forEach(tag => {
              tag.addEventListener('click', () => {
                  const tagName = tag.textContent;

                  // Toggle "tag" buttons on click.
                  if (tag.classList.contains('btn-primary')) {
                      tag.classList.replace('btn-primary', 'btn-outline-primary');
                  } else {
                      tag.classList.replace('btn-outline-primary', 'btn-primary');
                      // If any other button gets activated, "all" gets deactivated.
                      allButton.classList.replace('btn-primary', 'btn-outline-primary');
                  }

                  panels.forEach(element => {
                      // If a tag gets clicked and a respective panel has this tag
                      if (element.hasAttribute('data-tags') &&
                          ! element.dataset.tags.includes(tagName)) {

                          // then toggle visibility
                          if (element.style.cssText.includes('none')) {
                              element.style.cssText = 'display: flex !important'
                          } else {
                              element.style.cssText = 'display: none !important'
                          }
                      }
                  });
              });
          });
      });
});

