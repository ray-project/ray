// Dismissable banner functionality. Each banner in the announcement region
// carries a `data-banner-key`, which doubles as its localStorage key, so
// dismissing one leaves the others visible.
document.addEventListener('DOMContentLoaded', function () {
  const region = document.querySelector('.bd-header-announcement');
  if (!region) {
    return;
  }

  const banners = Array.from(region.querySelectorAll('[data-banner-key]'));

  function hide(banner) {
    banner.style.display = 'none';
    // Collapse the region once nothing is left, so no empty strip remains.
    if (banners.every((other) => other.style.display === 'none')) {
      region.style.display = 'none';
    }
  }

  banners.forEach(function (banner) {
    const bannerKey = banner.dataset.bannerKey;

    if (localStorage.getItem(bannerKey) === 'true') {
      hide(banner);
      return;
    }

    const closeButton = banner.querySelector('.ray-banner__close');
    if (closeButton) {
      closeButton.addEventListener('click', function () {
        localStorage.setItem(bannerKey, 'true');
        hide(banner);
      });
    }
  });
});
