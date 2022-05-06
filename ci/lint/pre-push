#!/bin/sh

echo "Linting changes as part of pre-push hook"
echo ""
echo "ci/lint/format.sh:"
ci/lint/format.sh

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
