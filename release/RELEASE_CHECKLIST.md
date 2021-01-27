# Release Checklist
This checklist is meant to be used in conjunction with the RELEASE_PROCESS.rst document.

## Initial Steps
- [ ] Called for release blockers
	- [ ] Messaged Ant about release blockers
- [ ] Announced branch cut date and estimated release date

## Branch Cut
- [ ] Release branch created
- [ ] PR created to update “latest” version on master (do not merge yet)
- [ ] Release branch versions updated
	- [ ] Version keys have new version
	- [ ] Update of “Latest” commits cherry-picked into release branch
- [ ] Release commits pulled into spreadsheet
- [ ] Release notes doc created
- [ ] Call for release notes made in Slack

## Release Testing
- [ ] Microbenchmark
	- [ ] Test passing
	- [ ] Results added to `release/release_logs`
- [ ] Long Running Tests (mark complete when run 24 hrs no issues)
	- [ ] actor_deaths
	- [ ] apex
	- [ ] impala
	- [ ] many_actor_tasks
	- [ ] many_drivers
	- [ ] many_ppo
	- [ ] many_tasks_serialized_ids
	- [ ] many_tasks
	- [ ] node_failures
	- [ ] pbt
	- [ ] serve_failure
	- [ ] serve
- [ ] Long Running Distributed Tests
	- [ ] pytorch_pbt_failure
- [ ] horovod_test
- [ ] Stress Tests
	- [ ] test_dead_actors
		- [ ] succeeds
		- [ ] Results added to `release/release_logs`
	- [ ] test_many_tasks
		- [ ] succeeds
		- [ ] Results added to `release/release_logs`
	- [ ] test_placement_group
		- [ ] succeeds
		- [ ] Results added to `release/release_logs`
- [ ] RLlib Tests
	- [ ] regression_tests
		- [ ] compact-regression-tests-tf
			- [ ] 	succeeds
			- [ ] Results added to `release/release_logs`
		- [ ] compact-regression-tests-torch
			- [ ] 	succeeds
			- [ ] Results added to `release/release_logs`
	- [ ] stress_tests
	- [ ] unit_gpu_tests
- [ ] ASAN Test
- [ ] K8s Test
	- [ ] K8s cluster launcher test
	- [ ] K8s operator test

## Final Steps
- [ ] Wheels uploaded to Test PyPI
- [ ] Wheel sanity checks with Test PyPI
	- [ ] Windows
		- [ ] Python 3.6
		- [ ] Python 3.7
		- [ ] Python 3.8
	- [ ] OSX
		- [ ] Python 3.6
		- [ ] Python 3.7
		- [ ] Python 3.8
	- [ ] Linux
		- [ ] Python 3.6
		- [ ] Python 3.7
		- [ ] Python 3.8
- [ ] Release is created on Github with release notes
	- [ ] Release includes contributors
	- [ ] Release notes sent for review to team leads
	- [ ] Release is published
- [ ] Wheels uploaded to production PyPI
	- [ ] Installing latest with `pip install -U ray` reveals correct version number and commit hash
- [ ] “Latest” docs point to new release version
- [ ] Docker image latest is updated on dockerhub
- [ ] PR to bump master version is merged
- [ ] Release is announced internally
- [ ] Release is announced externally
- [ ] Any code/doc changes made during the release process contributed back to master branch