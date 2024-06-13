/**
 * Get the status (checked/unchecked) for each filter.
 *
 * @returns {Object} Arrays of the name and status of each filter, grouped together into filter
 * groups.
 */
function getFilterStatuses() {
  const useCases = Array.from(
    document.querySelectorAll('#use-case-dropdown .checkbox-container'),
  ).map((label) => {
    return {
      name: label.textContent.toLowerCase(),
      isChecked: label.querySelector('input').checked,
    };
  });
  const libraries = Array.from(
    document.querySelectorAll('#library-dropdown .checkbox-container'),
  ).map((label) => {
    return {
      name: label.textContent.toLowerCase(),
      isChecked: label.querySelector('input').checked,
    };
  });
  const frameworks = Array.from(
    document.querySelectorAll('#framework-dropdown .checkbox-container'),
  ).map((label) => {
    return {
      name: label.textContent.toLowerCase(),
      isChecked: label.querySelector('input').checked,
    };
  });
  const contributor = Array.from(
    document.querySelectorAll('#all-examples-dropdown .checkbox-container'),
  ).map((label) => {
    const inputElement = label.querySelector('input');
    return {
      name: inputElement.id.replace('-checkbox', ''),
      isChecked: inputElement.checked,
    };
  });
  return {
    useCases,
    libraries,
    frameworks,
    contributor,
  };
}

/**
 * Test whether the tags of the given example panel match the requested filters.
 *
 * @param {any} tags Tags of the example panel
 * @param {any} filters Filter statuses for all the filter groups; this should be the output of
 * getFilterStatuses.
 * @returns {bool} True if the example panel matches the filters, or not.
 */
function panelMatchesFilters(tags, filters) {
  return Object.entries(filters).every(([group, groupTags]) => {
    // If there is no selection, consider the panel to be matched
    if (groupTags.filter(({isChecked}) => isChecked).length === 0) {
      return true;
    }

    // If "Any" is checked, consider the panel to be matched
    if (
      groupTags.filter(({name, isChecked}) => name === 'any' && isChecked)
        .length > 0
    ) {
      return true;
    }

    // Otherwise show the panel if any checked item matches the tags of the panel
    return groupTags
      .filter(({isChecked}) => isChecked)
      .some(({name}) => tags.includes(name));
  });
}

/** Apply the currently selected filters to the example gallery, showing only the relevant examples. */
function applyFilter() {
  const noMatchesElement = document.getElementById('no-matches');
  const panels = document.querySelectorAll('.example');
  const filters = getFilterStatuses();
  const searchTerm = document
    .getElementById('examples-search-input')
    .value.toLowerCase();

  // Show all panels before hiding the ones that need to be hidden.
  panels.forEach((panel) => panel.classList.remove('hidden'));

  // Check the title and tags of each example panel. If the tags match and the search term matches,
  // show the panel.
  panels.forEach((panel) => {
    const title = panel
      .querySelector('.example-title')
      .textContent.toLowerCase();
    const tags = panel
      .querySelector('.example-tags')
      .textContent.toLowerCase()
      .concat();
    const other_keywords = panel
      .querySelector('.example-other-keywords')
      .textContent.toLowerCase();
    const keywords = `${tags} ${other_keywords}`;
    const matchesSearch =
      title.includes(searchTerm) || keywords.includes(searchTerm);
    const matchesTags = panelMatchesFilters(keywords, filters);

    // Hide panels that have no match
    if (matchesSearch && matchesTags) {
      panel.classList.remove('hidden');
    } else {
      panel.classList.add('hidden');
    }
  });

  // If none are shown, show the "no matches" graphic.
  if (document.querySelectorAll('.example:not(.hidden)').length === 0) {
    noMatchesElement.classList.remove('hidden');
  } else {
    noMatchesElement.classList.add('hidden');
  }

  // Set the URL to match the active filters using query parameters.
  const selectedTags = Object.entries(filters).map(([group, groupTags]) => {
    return {
      group,
      selected: groupTags
        .filter(({isChecked}) => isChecked)
        .map(({name}) => name),
    };
  });

  const queryParam = selectedTags
    .filter(({group, selected}) => selected.length > 0)
    .map(({group, selected}) => `${group}=${selected.join(',')}`)
    .join('&');

  history.replaceState(
    null,
    null,
    queryParam.length === 0 ? location.pathname : `?${queryParam}`,
  );
}

window.addEventListener('load', () => {
  // Listen for filter checkbox clicks.
  document.querySelectorAll('.filter-checkbox').forEach((tag) => {
    tag.addEventListener('click', () => applyFilter());
  });

  // Add event listener for keypresses in the search bar.
  document
    .getElementById('examples-search-input')
    .addEventListener('keyup', (event) => {
      event.preventDefault();
      applyFilter();
    });

  // Add the ability to provide URL query parameters to filter examples on page load.
  const urlParams = new URLSearchParams(window.location.search);
  if (urlParams.size > 0) {
    urlParams.forEach((params) => {
      params.split(',').forEach((param) => {
        const element = document.getElementById(`${param}-checkbox`);
        if (element) {
          element.checked = true;
        }
      });
    });
  }

  // Apply the filter in case there are URL query parameters.
  applyFilter();

  const dropdowns = Array.from(
    document.querySelectorAll('.dropdown-content'),
  ).map((dropdown) => {
    return {
      dropdown,
      input: dropdown.parentNode.querySelector('input'),
      inputContainer: dropdown.parentNode,
    };
  });
  document.addEventListener('click', (event) => {
    let targetEl = event.target; // clicked element

    do {
      const unclicked = dropdowns.filter(({dropdown, inputContainer}) => {
        return !(targetEl == dropdown || targetEl == inputContainer);
      });
      if (unclicked.length !== dropdowns.length) {
        // There has been a click inside one of the dropdowns. Close unclicked dropdowns and return.
        unclicked.forEach(({input}) => {
          input.checked = false;
        });
        return;
      }

      // Go up the DOM.
      targetEl = targetEl.parentNode;
    } while (targetEl);

    // This is a click outside. Close all dropdowns.
    dropdowns.forEach(({dropdown, input}) => {
      input.checked = false;
    });
  });
});
