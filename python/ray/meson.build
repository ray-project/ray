# === @begin: Embed commit into `_version.py` ===
custom_target(
    input: '_version.py.in',
    output: '_version.py',
    command: ['sed', f's/{{RAY_COMMIT_SHA}}/@ray_commit_sha@/g', '@INPUT@'],
    capture: true,
    install_dir: py.get_install_dir() / 'ray',
    install_tag: 'python-runtime',
    install: true,
)
# === @end: Embed commit into `_version.py` ===