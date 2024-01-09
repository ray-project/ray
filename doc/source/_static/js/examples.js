/**
 * Check whether a panel matches the selected filter tags.
 *
 * @param {any} panel Example gallery item
 * @param {Array<Array<string>>} groupedActiveTags Groups of tags selected by the user.
 * @returns {boolean} True if the panel should be shown, false otherwise
 */
function panelMatchesTags(panel, groupedActiveTags) {
  // Show the panel if every tagGroup has at least one active tag in the classList,
  // or if no tag in a group is selected.
  return groupedActiveTags.every(tagGroup => {
    return tagGroup.length === 0 || Array.from(panel.classList).some(tag => tagGroup.includes(tag))
  })
}


window.addEventListener('load', () => {

  /* Fetch the tags that the user can filter on from the buttons in the sidebar
   * Additionally retrieve the elements that we need for filtering.
   */
  const tags = {}
  document.querySelectorAll('div.tag-section').forEach(group => {
    tags[group.id] = group.querySelectorAll('div.tag-group > div.tag.btn')
  })
  const noMatchesElement = document.querySelector("#noMatches");
  const panels = document.querySelectorAll('.gallery-item')

  /**
   * Filter the links to the examples in the example gallery
   * by the selected tags and the current search query.
   */
  function filterPanels() {
    const query = document.getElementById("searchInput").value.toLowerCase();
    const activeTags = Array.from(document.querySelectorAll('.tag.btn-primary')).map(el => el.id);
    const groupedActiveTags = Object.values(tags).map(group => {
      const tagNames = Array.from(group).map(element => element.id);
      return activeTags.filter(activeTag => tagNames.includes(activeTag));
    })

    // Show all panels first
    panels.forEach(panel => panel.classList.remove("hidden"));

    let toHide = [];
    let toShow = [];

    // Show each panel if it has every active tag and matches the search query
    panels.forEach(panel => {
      const text = (panel.textContent + panel.classList.toString()).toLowerCase();
      // const hasTag = activeTags.every(tag => panel.classList.contains(tag));
      const hasTag = panelMatchesTags(panel, groupedActiveTags)
      const hasText = text.includes(query.toLowerCase());

      if (hasTag && hasText) {
        toShow.push(panel);
      } else {
        toHide.push(panel);
      }
    })

    toShow.forEach(panel => panel.classList.remove("hidden"));
    toHide.forEach(panel => panel.classList.add("hidden"));

    // If no matches are found, display the noMatches element
    if (toShow.length === 0) {
        noMatchesElement.classList.remove("hidden");
      } else {
        noMatchesElement.classList.add("hidden");
    }

    // Set the URL to match the active tags using query parameters
    history.replaceState(null, null, activeTags.length === 0 ? location.pathname : `?tags=${activeTags.join(',')}`);
  }

  // Generate the callback triggered when a user clicks on a tag filter button.
  document.querySelectorAll('.tag').forEach(tag => {
    tag.addEventListener('click', () => {
      // Toggle "tag" buttons on click.
      if (tag.classList.contains('btn-primary')) {
          // deactivate filter button
          tag.classList.replace('btn-primary', 'btn-outline-primary');
      } else {
          // activate filter button
          tag.classList.replace('btn-outline-primary', 'btn-primary');
      }
      filterPanels()
    });
  });

  // Add event listener for keypresses in the search bar
  const searchInput = document.getElementById("searchInput");
  if (searchInput) {
      searchInput.addEventListener("keyup", function (event) {
          event.preventDefault();
          filterPanels();
      });
  }

  // Add the ability to provide URL query parameters to filter examples on page load
  const urlParams = new URLSearchParams(window.location.search);
  if (urlParams.size > 0) {
      const urlTagParams = urlParams.get('tags').split(',');
      urlTagParams.forEach(tag => {
          const tagButton = document.getElementById(tag);
          if (tagButton) {
            tagButton.classList.replace('btn-outline-primary', 'btn-primary');
          }
      });
      filterPanels();
  }
});
