#include "ray/raylet/scheduling/fixed_point.h"

#include <cmath>

FixedPoint::FixedPoint(double d) { i_ = (uint64_t)(d * RESOURCE_UNIT_SCALING); }

FixedPoint::FixedPoint(int i) { i_ = (i * RESOURCE_UNIT_SCALING); }

FixedPoint::FixedPoint(uint32_t i) { i_ = (i * RESOURCE_UNIT_SCALING); }

FixedPoint::FixedPoint(int64_t i) : FixedPoint((double)i) {}

FixedPoint::FixedPoint(uint64_t i) : FixedPoint((double)i) {}

FixedPoint FixedPoint::operator+(FixedPoint const &ru) const {
  FixedPoint res;
  res.i_ = i_ + ru.i_;
  return res;
}

FixedPoint FixedPoint::operator+=(FixedPoint const &ru) {
  i_ += ru.i_;
  return *this;
}

FixedPoint FixedPoint::operator-(FixedPoint const &ru) const {
  FixedPoint res;
  res.i_ = i_ - ru.i_;
  return res;
}

FixedPoint FixedPoint::operator-=(FixedPoint const &ru) {
  i_ -= ru.i_;
  return *this;
}

FixedPoint FixedPoint::operator-() const {
  FixedPoint res;
  res.i_ = -i_;
  return res;
}

FixedPoint FixedPoint::operator+(double const d) const {
  FixedPoint res;
  res.i_ = i_ + (int64_t)(d * RESOURCE_UNIT_SCALING);
  return res;
}

FixedPoint FixedPoint::operator-(double const d) const {
  FixedPoint res;
  res.i_ = i_ - (int64_t)(d * RESOURCE_UNIT_SCALING);
  return res;
}

FixedPoint FixedPoint::operator=(double const d) {
  i_ = (int64_t)(d * RESOURCE_UNIT_SCALING);
  return *this;
}

FixedPoint FixedPoint::operator+=(double const d) {
  i_ += (int64_t)(d * RESOURCE_UNIT_SCALING);
  return *this;
}

FixedPoint FixedPoint::operator+=(int64_t const ru) {
  *this += (double)ru;
  return *this;
}

bool FixedPoint::operator<(FixedPoint const &ru1) const { return (i_ < ru1.i_); };
bool FixedPoint::operator>(FixedPoint const &ru1) const { return (i_ > ru1.i_); };
bool FixedPoint::operator<=(FixedPoint const &ru1) const { return (i_ <= ru1.i_); };
bool FixedPoint::operator>=(FixedPoint const &ru1) const { return (i_ >= ru1.i_); };
bool FixedPoint::operator==(FixedPoint const &ru1) const { return (i_ == ru1.i_); };
bool FixedPoint::operator!=(FixedPoint const &ru1) const { return (i_ != ru1.i_); };

std::ostream &operator<<(std::ostream &out, FixedPoint const &ru1) {
  out << ru1.i_;
  return out;
}

double FixedPoint::Double() const { return round(i_) / RESOURCE_UNIT_SCALING; };
