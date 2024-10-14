# Release Process

The Kubernetes controller-runtime Project is released on an as-needed basis. The process is as follows:

**Note:** Releases are done from the `release-MAJOR.MINOR` branches. For PATCH releases is not required
to create a new branch you will just need to ensure that all big fixes are cherry-picked into the respective
`release-MAJOR.MINOR` branch. To know more about versioning check https://semver.org/. 

## How to do a release 

### Create the new branch and the release tag

1. Create a new branch `git checkout -b release-<MAJOR.MINOR>` from master
2. Push the new branch to the remote repository

### Now, let's generate the changelog

1. Create the changelog from the new branch `release-<MAJOR.MINOR>` (`git checkout release-<MAJOR.MINOR>`). 
You will need to use the [kubebuilder-release-tools][kubebuilder-release-tools] to generate the notes. See [here][release-notes-generation]

> **Note**
> - You will need to have checkout locally from the remote repository the previous branch
> - Also, ensure that you fetch all tags from the remote `git fetch --all --tags`

### Draft a new release from GitHub

1. Create a new tag with the correct version from the new `release-<MAJOR.MINOR>` branch 
2. Add the changelog on it and publish. Now, the code source is released !

### Add a new Prow test the for the new branch release

1. Create a new prow test under [github.com/kubernetes/test-infra/tree/master/config/jobs/kubernetes-sigs/controller-runtime](https://github.com/kubernetes/test-infra/tree/master/config/jobs/kubernetes-sigs/controller-runtime) 
for the new `release-<MAJOR.MINOR>` branch. (i.e. for the `0.11.0` release see the PR: https://github.com/kubernetes/test-infra/pull/25205)
2. Ping the infra PR in the controller-runtime slack channel for reviews.

### Announce the new release:

1. Publish on the Slack channel the new release, i.e:

````
:announce: Controller-Runtime v0.12.0 has been released!
This release includes a Kubernetes dependency bump to v1.24.
For more info, see the release page: https://github.com/kubernetes-sigs/controller-runtime/releases.
 :tada:  Thanks to all our contributors!
````

2. An announcement email is sent to `kubebuilder@googlegroups.com` with the subject `[ANNOUNCE] Controller-Runtime $VERSION is released`
