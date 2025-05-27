#!/bin/env/bash
source .codebase/patch/_codebase_prepare.sh
MINIMAL_INSTALL=1 PYTHON=3.9 source ci/env/install-dependencies.sh
pip install click==8.1.3
pip install flake8==3.9.1
pip install black==22.10.0
pip install mypy==1.7.0
pip install isort==5.10.1
pip install types-PyYAML==6.0.12.2
pip install flake8-comprehensions==3.10.1
pip install flake8-quotes==2.0.0
pip install flake8-bugbear==21.9.2
echo "Linting changes as part of pre-push hook"
echo "ci/lint/format.sh:"
scripts/format.sh --all

lint_exit_status=$?

git diff --color | cat

if [ $lint_exit_status -ne 0 ]; then
	echo ""
	echo "Linting changes failed."
	echo "Please make sure 'ci/lint/format.sh'"\
		"runs with no errors before pushing."
	echo "If you want to ignore this and push anyways,"\
		"re-run with '--no-verify'."
	exit 1
fi
exit 0

