#!/usr/bin/env bash

# Tips:
# - TRAVIS set to true
# - TRAVIS_COMMIT is filled with Build.SourceVersion
# - TRAVIS_BRANCH is filled with one of the following variables:
#   * Build.SourceBranch
#   * System.PullRequest.TargetBranch
# - TRAVIS_PULL_REQUEST is filled with one of the following variables:
#   * Build.SourceVersion
#   * System.PullRequest.PullRequestNumber
# - TRAVIS_EVENT_TYPE is determined at tuntime based on the variable Build.Reason
# - TRAVIS_COMMIT_RANGE is filled with Build.SourceVersion
# - TRAVIS_OS_NAME is assumed already defined
# - TRAVIS_BUILD_DIR got replaced by Build.SourcesDirectory

# Cause the script to exit if a single command fails.
set -e

# TODO: [CI] remove after CI get stable
set -x

# Initialize travis script expected variables.
export PYTHON=$PYTHON_VERSION
echo "Determined PYTHON variable: $PYTHON"

export TRAVIS_COMMIT=$BUILD_SOURCEVERSION
echo "Determined TRAVIS_COMMIT variable: $TRAVIS_COMMIT"

export TRAVIS_BRANCH=$BUILD_SOURCEBRANCH
echo "Determined SYSTEM_PULLREQUEST_TARGETBRANCH variable: $SYSTEM_PULLREQUEST_TARGETBRANCH"
echo "Determined BUILD_SOURCEBRANCH variable: $BUILD_SOURCEBRANCH"
echo "Determined TRAVIS_BRANCH variable: $TRAVIS_BRANCH"

export TRAVIS_PULL_REQUEST=$SYSTEM_PULLREQUEST_PULLREQUESTNUMBER && [[ -z $TRAVIS_PULL_REQUEST ]] && TRAVIS_PULL_REQUEST=$BUILD_SOURCEVERSION
echo "Determined TRAVIS_PULL_REQUEST variable: $TRAVIS_PULL_REQUEST"

export TRAVIS_EVENT_TYPE="push" && [[ ${BUILD_REASON:-X} == "PullRequest" ]] && TRAVIS_EVENT_TYPE="pull_request"
echo "Determined TRAVIS_EVENT_TYPE variable: $TRAVIS_EVENT_TYPE"

export TRAVIS_COMMIT_RANGE=$BUILD_SOURCEVERSION
echo "Determined TRAVIS_COMMIT_RANGE variable: $TRAVIS_COMMIT_RANGE"

echo "Determined TRAVIS_OS_NAME variable: $TRAVIS_OS_NAME"

export TRAVIS_BUILD_DIR=$BUILD_SOURCESDIRECTORY
echo "Determined TRAVIS_BUILD_DIR variable: $TRAVIS_BUILD_DIR"

export CI=true


# TODO: [CI] remove this step after adding a condition in 
# ci/travis/install-dependencies.sh that check first if 
# node is already installed before install it
if which node > /dev/null
then
    echo $(node --version)
    echo "node is installed, skipping..."
else
    echo "node not installed, installing nvm..."
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
    echo "nvm sh downloaded and applied."
fi
