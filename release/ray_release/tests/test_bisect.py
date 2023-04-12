from unittest import mock
from ray_release.scripts.ray_bisect import _bisect


def test_bisect():
    test_cases = {
        "c3": {
            "c0": True,
            "c1": True,
            "c3": False,
            "c4": False,
        },
        "c1": {
            "c0": True,
            "c1": False,
        },
    }

    for output, input in test_cases.items():

        def _mock_run_test(test_name: str, commit: str) -> bool:
            return input[commit]

        with mock.patch(
            "ray_release.scripts.ray_bisect._run_test",
            side_effect=_mock_run_test,
        ), mock.patch(
            "ray_release.scripts.ray_bisect._get_test",
            return_value={},
        ):
            assert _bisect("test", list(input.keys())) == output

def test_bisect():
    commit_to_test_result = {
      'c0': True, 
      'c1': True, 
      'c2': True, 
      'c3': False, 
      'c4': False, 
    }
    def _mock_get_commit_lists(passing_commit: str, failing_commit: str) -> List[str]:
        commits = []
        in_range = False
        for commit in commit_to_test_result:
            if commit == failing_commit:
                break
            if in_range:
                commits.append(commit)
            if commit == passing_commit:
                commits.append(commit)
                in_range = True
        return commits
    def _mock_run_test(test_name: str, commit: str) -> bool:
        return commit_to_test_result[commit]
    with mock.patch(
        'ray_release.scripts.ray_bisect._get_commit_lists',
        side_effect=_mock_get_commit_lists,
    ), mock.patch(
        'ray_release.scripts.ray_bisect._run_test',
        side_effect=_mock_run_test,
    ):
        blamed_commit = _bisect('test', 'c0', 'c4')
        assert blamed_commit == 'c3'
