set -e

function inline_link {
  LINK=$(printf "url='%s'" "$1")

  if [ $# -gt 1 ]; then
    LINK=$(printf "$LINK;content='%s'" "$2")
  fi

  printf '\033]1339;%s\a\n' "$LINK"
}

pip install -r requirements.txt
python make_screenshot.py
buildkite-agent artifact upload dashboard_render.png
inline_image 'artifact:dashboard_render.png' 'Rendering of Ray Dashboard'
