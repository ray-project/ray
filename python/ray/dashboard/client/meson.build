run_command(npm, 'ci', check: true)
run_command(npm, 'run', 'build', check: true)

install_subdir(
    'build/',
    install_dir: py.get_install_dir() / 'ray/dashboard/client/',
    install_tag: 'python-runtime'
)