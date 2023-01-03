let panels = [];
let tags = [];

window.addEventListener('load', () => {

    fetch('/_static/tag-mapping.json')
      .then(response => response.json())
      .then(classTagMap => {

          for (const className in classTagMap) {
              let element = document.getElementsByClassName(className)[0];
              for (let i = 0; i < 4 && parent; i++) {
                  element = element.parentElement;
                  element.setAttribute('data-tags', classTagMap[className]);
              }
          }

          panels = document.querySelectorAll('.d-flex.docutils');
          tags = document.querySelectorAll('.tag');

          tags.forEach(tag => {
              tag.addEventListener('click', () => {
                  const tagName = tag.textContent;

                  if (tag.classList.contains('btn-primary')) {
                      tag.classList.replace('btn-primary', 'btn-outline-primary');
                  } else {
                      tag.classList.replace('btn-outline-primary', 'btn-primary');
                  }

                  panels.forEach(element => {
                      if (element.hasAttribute('data-tags') &&
                          ! element.dataset.tags.includes(tagName)) {

                          // toggle visibility
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

