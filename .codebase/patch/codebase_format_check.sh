#!/bin/env/bash
source .codebase/patch/_codebase_prepare.sh
MINIMAL_INSTALL=1 PYTHON=3.9 source ci/travis/install-dependencies.sh || MINIMAL_INSTALL=1 PYTHON=3.9 source ci/env/install-dependencies.sh
pip install -r python/requirements_linters.txt
pip install --upgrade click==8.0.2

echo "Linting changes as part of pre-push hook"
echo "ci/lint/format.sh:"
scripts/format.sh --all

lint_exit_status=$?
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

