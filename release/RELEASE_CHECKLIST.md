# Release Checklist
This checklist is meant to be used in conjunction with the RELEASE_PROCESS.rst document.

## Initial Steps
- [ ] Called for release blockers
	- [ ] Messaged Ant about release blockers
- [ ] Announced branch cut date and estimated release date

## Branch Cut
- [ ] Release branch created
- [ ] Release branch versions updated
	- [ ] Version keys have new version
	- [ ] Update of “Latest” commits cherry-picked into release branch
- [ ] Release commits pulled into spreadsheet
- [ ] Release notes doc created
- [ ] Call for release notes made in Slack

## Release Testing
- [ ] ``core-nightly`` release test suite
    - [ ] Test passing
    - [ ] Results added to `release/release_logs`
        - [ ] many_actors
        - [ ] many_nodes
        - [ ] many_pgs
        - [ ] object_store
        - [ ] single_node
        - [ ] stress_test_dead_actors
        - [ ] stress_test_many_tasks
        - [ ] stress_test_placement_group
- [ ] ``nightly`` release test suite
    - [ ] Test passing
    - [ ] Results added to `release/release_logs`
        - [ ] microbenchmark
- [ ] `kubernetes` manual release tests pass

- [ ] ``weekly`` release test suite
    - [ ] Test passing

## Final Steps
- [ ] Check version strings once more
- [ ] Anyscale Docker images built and deployed
- [ ] ML Docker Image Updated
- [ ] Wheels uploaded to Test PyPI
- [ ] Wheel sanity checks with Test PyPI
    - [ ] Windows
        - [ ] Python 3.6
        - [ ] Python 3.7
        - [ ] Python 3.8
        - [ ] Python 3.9
    - [ ] OSX
        - [ ] Python 3.6
        - [ ] Python 3.7
        - [ ] Python 3.8
        - [ ] Python 3.9
    - [ ] Linux
        - [ ] Python 3.6
        - [ ] Python 3.7
        - [ ] Python 3.8
        - [ ] Python 3.9
- [ ] Release is created on Github with release notes
    - [ ] Release includes contributors
    - [ ] Release notes sent for review to team leads
    - [ ] Release is published
- [ ] Wheels uploaded to production PyPI
    - [ ] Installing latest with `pip install -U ray` reveals correct version number and commit hash
- [ ] "Latest" docs point to new release version
- [ ] Docker image latest is updated on dockerhub
- [ ] Java release is published on Maven
- [ ] Release is announced internally
- [ ] Release is announced externally
- [ ] Any code/doc changes made during the release process contributed back to master branch
