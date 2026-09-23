// Dismissable banner functionality. Each banner in the announcement region
// carries a `data-banner-key`, which doubles as its localStorage key, so
// dismissing one leaves the others visible.
document.addEventListener('DOMContentLoaded', function () {
  const region = document.querySelector('.bd-header-announcement');
  if (!region) {
    return;
  }

  const banners = Array.from(region.querySelectorAll('[data-banner-key]'));

  // localStorage access throws in restricted environments (Safari private mode,
  // strict privacy settings), so guard every read and write. A failure here just
  // means the banner isn't remembered as dismissed, which is the safe fallback.
  function isDismissed(bannerKey) {
    try {
      return localStorage.getItem(bannerKey) === 'true';
    } catch (e) {
      return false;
    }
  }

  function markDismissed(bannerKey) {
    try {
      localStorage.setItem(bannerKey, 'true');
    } catch (e) {
      /* storage unavailable; the banner reappears on the next load */
    }
  }

  function hide(banner) {
    banner.style.display = 'none';
    // Collapse the region once nothing is left, so no empty strip remains.
    if (banners.every((other) => other.style.display === 'none')) {
      region.style.display = 'none';
    }
  }

  banners.forEach(function (banner) {
    const bannerKey = banner.dataset.bannerKey;

    if (isDismissed(bannerKey)) {
      hide(banner);
      return;
    }

    const closeButton = banner.querySelector('.ray-banner__close');
    if (closeButton) {
      closeButton.addEventListener('click', function () {
        markDismissed(bannerKey);
        hide(banner);
      });
    }
  });
});
