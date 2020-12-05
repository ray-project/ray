set -e

function inline_image {
  printf '\033]1338;url='"$1"';alt='"$2"'\a\n'
}

pip install -r requirements.txt
python make_screenshot.py
buildkite-agent artifact upload dashboard_render.png
inline_image 'artifact://dashboard_render.png' 'Rendering of Ray Dashboard'
