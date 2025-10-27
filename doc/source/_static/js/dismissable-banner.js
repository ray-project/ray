// Dismissable banner functionality
document.addEventListener('DOMContentLoaded', function () {
  const banner = document.querySelector('.bd-header-announcement');
  const closeButton = document.getElementById('close-banner');
  const bannerKey = 'ray-docs-banner-dismissed';

  // Check if banner was previously dismissed
  if (localStorage.getItem(bannerKey) === 'true') {
    if (banner) {
      banner.style.display = 'none';
    }
    return;
  }

  // Add click handler for close button
  if (closeButton) {
    closeButton.addEventListener('click', function () {
      if (banner) {
        banner.style.display = 'none';
        localStorage.setItem(bannerKey, 'true');
      }
    });
  }
});
