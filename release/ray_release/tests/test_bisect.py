from typing import List
from ray_release.scripts.ray_bisect import (
  _bisect
)

def test_bisect():
    commit_to_test_result = {
      'c0': True, 
      'c1': True, 
      'c2': True, 
      'c3': False, 
      'c4': False, 
    }
    def _mock_get_commit_lists(passing_commit: str, failing_commit: str) -> List[str]:
    blamed_commit = _bisect('test', 'c0', 'c4')
    assert blamed_commit == 'c3'