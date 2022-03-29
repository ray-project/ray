#include "ray/common/task/task_priority.h"

namespace ray {

int Priority::childless_id = childless_id_start;
int Priority::id = 0;

int Priority::extend(int64_t size) const {
  int64_t diff = size -  static_cast<int64_t>(score.size());
  if (diff > 0) {
    for (int64_t i = 0; i < diff; i++) {
      score.push_back(INT_MAX);
    }
  }
  return diff;
}

void Priority::SetFromParentPriority(Priority &parent){
  if(parent.score.size() == 1 && parent.score[0] == INT_MAX){
	score[0] = childless_id++;
  }else{
    //Change parent priority start from id if it is from childless
    if(parent.score[0] >= childless_id_start){
      parent.score[0] = id++;
    }
	score = parent.score;
    score.push_back(id++);
  }
}

bool Priority::operator<(const Priority &rhs) const {
  int rhs_diff = rhs.extend(score.size());
  int lhs_diff = extend(rhs.score.size());

  bool ret = score < rhs.score;

  for(int i=0; i<rhs_diff; i++)
    rhs.score.pop_back();
  for(int i=0; i<lhs_diff; i++)
    score.pop_back();

  return ret;
}

bool Priority::operator<=(const Priority &rhs) const {
  int rhs_diff = rhs.extend(score.size());
  int lhs_diff = extend(rhs.score.size());

  bool ret = score <= rhs.score;

  for(int i=0; i<rhs_diff; i++)
    rhs.score.pop_back();
  for(int i=0; i<lhs_diff; i++)
    score.pop_back();

  return ret;
}

bool Priority::operator>(const Priority &rhs) const {
  int rhs_diff = rhs.extend(score.size());
  int lhs_diff = extend(rhs.score.size());

  bool ret = score > rhs.score;

  for(int i=0; i<rhs_diff; i++)
    rhs.score.pop_back();
  for(int i=0; i<lhs_diff; i++)
    score.pop_back();

  return ret;
}

bool Priority::operator>=(const Priority &rhs) const {
  int rhs_diff = rhs.extend(score.size());
  int lhs_diff = extend(rhs.score.size());

  bool ret = score >= rhs.score;

  for(int i=0; i<rhs_diff; i++)
    rhs.score.pop_back();
  for(int i=0; i<lhs_diff; i++)
    score.pop_back();

  return ret;
}

std::ostream &operator<<(std::ostream &os, const Priority &p) {
  os << "[ ";
  for (const auto &i : p.score) {
    os << i << " ";
  }
  os << "]";
  return os;
}

std::ostream &operator<<(std::ostream &os, const TaskKey &k) {
  return os << k.second << " " << k.first;
}

}  // namespace ray
