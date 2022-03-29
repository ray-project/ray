#include "ray/common/task/task_priority.h"

namespace ray {

Priority::childless_id = childless_id_start;
Priority::id = 0;

void Priority::extend(int64_t size) const {
  int64_t diff = size -  static_cast<int64_t>(score.size());
  if (diff > 0) {
    for (int64_t i = 0; i < diff; i++) {
      score.push_back(INT_MAX);
    }
  }
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

  this->parent = &parent;
}

void Priority::IncreaseChildrenCount(){
	num_children++;
}

//TODO(Jae) change the comparison to if one has children while the other not
//prioritize one with children. Not num of children
bool Priority::operator<(const Priority &rhs) const {
  if(parent == NULL && rhs.parent == NULL){
	  if(num_children != 0){
		  if(rhs.num_children == 0){
			  return true;
		  }
	  }else{
		  if(rhs.num_children != 0){
			  return false;
		  }
	  }
  }else{
	  rhs.extend(score.size());
	  extend(rhs.score.size());
  }

  return score < rhs.score;
}

bool Priority::operator<=(const Priority &rhs) const {
  if(parent == NULL && rhs.parent == NULL){
	  if(num_children != 0){
		  if(rhs.num_children == 0){
			  return true;
		  }
	  }else{
		  if(rhs.num_children != 0){
			  return false;
		  }
	  }
  }else{
	  rhs.extend(score.size());
	  extend(rhs.score.size());
  }

  return score <= rhs.score;
}

bool Priority::operator>(const Priority &rhs) const {
  if(parent == NULL && rhs.parent == NULL){
	  if(num_children != 0){
		  if(rhs.num_children == 0){
			  return false;
		  }
	  }else{
		  if(rhs.num_children != 0){
			  return true;
		  }
	  }
  }else{
	  rhs.extend(score.size());
	  extend(rhs.score.size());
  }

  return score > rhs.score;
}

bool Priority::operator>=(const Priority &rhs) const {
  if(parent == NULL && rhs.parent == NULL){
	  if(num_children != 0){
		  if(rhs.num_children == 0){
			  return false;
		  }
	  }else{
		  if(rhs.num_children != 0){
			  return true;
		  }
	  }
  }else{
	  rhs.extend(score.size());
	  extend(rhs.score.size());
  }

  return score >= rhs.score;
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
