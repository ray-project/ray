// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/common/status_or.h"

#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <utility>

#include "ray/common/tests/testing.h"

namespace ray {

namespace {

struct TestClass {
 public:
  int val = 10;
};

// Check conversion of templated status or.
class Base {
 public:
  virtual ~Base() {}
};
class Derived : public Base {
 public:
  ~Derived() override {}
};

StatusOr<int> GetErrorStatus() { return Status::Invalid("Invalid error status."); }
StatusOr<int> GetValue() { return 1; }

// Tracks live instances by identity so a test can assert, deterministically and
// without a sanitizer, both that every constructed value is destroyed exactly
// once and that no operation runs a destructor on the wrong storage. `live`
// holds the address of each live instance: the destructor flags tearing down an
// address that was never recorded (destroying unconstructed or already-dead
// storage), and each scope asserts `live` is empty on exit (catching a value
// whose destructor was skipped). A single net counter cannot see the swap bug —
// the skipped destructor of one operand and the spurious ops on the other's
// unconstructed storage cancel out in the count.
struct Counted {
  static std::multiset<const void *> live;
  int value;
  explicit Counted(int v = 0) : value(v) { live.insert(this); }
  Counted(const Counted &o) : value(o.value) { live.insert(this); }
  Counted(Counted &&o) noexcept : value(o.value) { live.insert(this); }
  // Assignment reuses the existing object, so liveness is unchanged.
  Counted &operator=(const Counted &o) = default;
  Counted &operator=(Counted &&o) noexcept = default;
  ~Counted() {
    auto it = live.find(this);
    EXPECT_TRUE(it != live.end()) << "destroyed an instance that was not live";
    if (it != live.end()) {
      live.erase(it);
    }
  }
};

std::multiset<const void *> Counted::live;

}  // namespace

TEST(StatusOrTest, AssignTest) {
  // Assign with status.
  {
    auto code = StatusCode::InvalidArgument;
    StatusOr<int> status_or_val = Status::InvalidArgument("msg");
    EXPECT_FALSE(status_or_val.ok());
    EXPECT_EQ(status_or_val.status().code(), code);
  }

  // Assign with value.
  {
    int val = 20;
    StatusOr<int> status_or_val = val;
    EXPECT_TRUE(status_or_val.ok());
    EXPECT_EQ(status_or_val.value(), val);
    EXPECT_EQ(*status_or_val, val);
  }
  // Assign with non-copiable value.
  {
    auto get_derived = []() -> StatusOr<std::unique_ptr<Base>> {
      std::unique_ptr<Base> val = std::make_unique<Derived>();
      StatusOr<std::unique_ptr<Base>> status_or_val{std::move(val)};
      return status_or_val;
    };
    auto val = get_derived();
  }
}

TEST(StatusOrTest, CopyTest) {
  // Assign with status.
  {
    auto code = StatusCode::InvalidArgument;
    StatusOr<int> status_or_val = Status::InvalidArgument("msg");
    auto copied_status_or = status_or_val;
    EXPECT_FALSE(copied_status_or.ok());
    EXPECT_EQ(copied_status_or.status().code(), code);
  }

  // Assign with value.
  {
    int val = 20;
    StatusOr<int> status_or_val = val;
    auto copied_status_or = status_or_val;
    EXPECT_TRUE(copied_status_or.ok());
    EXPECT_EQ(copied_status_or.value(), val);
    EXPECT_EQ(*copied_status_or, val);
  }
}

TEST(StatusOrTest, MoveTest) {
  // Assign with status.
  {
    auto code = StatusCode::InvalidArgument;
    StatusOr<int> status_or_val = Status::InvalidArgument("msg");
    auto moved_status_or = std::move(status_or_val);
    EXPECT_FALSE(moved_status_or.ok());
    EXPECT_EQ(moved_status_or.status().code(), code);
  }

  // Assign with value.
  {
    int val = 20;
    StatusOr<int> status_or_val = val;
    auto moved_status_or = status_or_val;
    EXPECT_TRUE(moved_status_or.ok());
    EXPECT_EQ(moved_status_or.value(), val);
    EXPECT_EQ(*moved_status_or, val);
  }
}

TEST(StatusOrTest, OperatorTest) {
  // Test operator->
  StatusOr<TestClass> status_or_val{TestClass{}};
  EXPECT_EQ(status_or_val->val, 10);
}

TEST(StatusOrTest, EqualityTest) {
  {
    StatusOr<int> val1 = Status::InvalidArgument("msg");
    StatusOr<int> val2 = 20;
    EXPECT_NE(val1, val2);
  }

  {
    StatusOr<int> val1 = 20;
    StatusOr<int> val2 = 20;
    EXPECT_EQ(val1, val2);
  }

  {
    StatusOr<int> val1 = 40;
    StatusOr<int> val2 = 20;
    EXPECT_NE(val1, val2);
  }
}

TEST(StatusOrTest, ValueOrTest) {
  // OK status.
  {
    StatusOr<int> statusor = 10;
    EXPECT_EQ(statusor.value_or(100), 10);
  }
  // Error status.
  {
    StatusOr<int> statusor = Status::InvalidArgument("msg");
    EXPECT_EQ(statusor.value_or(100), 100);
  }
}

// Test StatusOr<Derived> to StatusOr<Base> conversion.
TEST(StatusOrTest, StatusOrConversion) {
  // Check value.
  {
    auto get_derived = []() -> StatusOr<std::unique_ptr<Derived>> {
      return std::make_unique<Derived>();
    };
    StatusOr<std::unique_ptr<Derived>> derived = get_derived();
    StatusOr<std::unique_ptr<Base>> base = std::move(derived);
  }
  // Check status.
  {
    Status status = Status::InvalidArgument("msg");
    StatusOr<std::unique_ptr<Derived>> derived{status};
    StatusOr<std::unique_ptr<Base>> base = std::move(derived);
    EXPECT_EQ(base.code(), status.code());
  }
}

TEST(StatusOrTest, StatusOrDefaultTest) {
  StatusOr<int> status_or_val = Status::InvalidArgument("msg");
  EXPECT_EQ(status_or_val.value_or_default(), 0);
}

TEST(StatusOrTest, AndThen) {
  auto f = [](const StatusOr<int> &statusor) {
    RAY_EXPECT_OK(statusor.status());
    return StatusOr<int>{statusor.value() + 10};
  };

  // Error status.
  {
    Status error_status = Status::InvalidArgument("msg");
    StatusOr<int> s = std::move(error_status);
    EXPECT_EQ(s.and_then(f).code(), StatusCode::InvalidArgument);
  }
  // OK status.
  {
    StatusOr<int> s = 10;
    EXPECT_EQ(s.and_then(f).value(), 20);
  }
}

TEST(StatusOrTest, OrElse) {
  auto f = [](const StatusOr<int> &statusor) {
    EXPECT_FALSE(statusor.ok());
    return StatusOr<int>{static_cast<int>(statusor.status().code())};
  };

  // Error status.
  {
    Status error_status = Status::InvalidArgument("msg");
    StatusOr<int> s = std::move(error_status);
    EXPECT_EQ(s.or_else(f).value(), static_cast<int>(StatusCode::InvalidArgument));
  }
  // OK status.
  {
    StatusOr<int> s = 10;
    EXPECT_EQ(s.or_else(f).value(), 10);
  }
}

TEST(StatusOrTest, AssignOrReturn) {
  // Get error status.
  {
    auto f = []() -> StatusOr<int> {
      RAY_ASSIGN_OR_RETURN(int val, GetValue());
      RAY_ASSIGN_OR_RETURN(val, GetErrorStatus());
      return val;
    };
    EXPECT_EQ(f().code(), StatusCode::Invalid);
  }
  // Get value.
  {
    auto f = []() -> StatusOr<int> {
      RAY_ASSIGN_OR_RETURN(int val, GetValue());
      return val;
    };
    EXPECT_EQ(f().value(), 1);
  }
}

TEST(StatusOrTest, CopyAssignment) {
  // Copy value.
  {
    StatusOr<int> val = 10;
    StatusOr<int> copy = val;
    EXPECT_EQ(val.value(), 10);
    EXPECT_EQ(copy.value(), 10);
  }

  // Error status.
  {
    StatusOr<int> val = Status::InvalidArgument("error");
    StatusOr<int> copy = val;
    EXPECT_EQ(val.code(), StatusCode::InvalidArgument);
    EXPECT_EQ(copy.code(), StatusCode::InvalidArgument);
  }
}

TEST(StatusOrTest, MoveAssignment) {
  // Move value.
  {
    StatusOr<int> val = 10;
    StatusOr<int> moved = std::move(val);
    EXPECT_EQ(moved.value(), 10);
  }

  // Move status.
  {
    StatusOr<int> val = Status::InvalidArgument("error");
    StatusOr<int> moved = std::move(val);
    EXPECT_EQ(moved.code(), StatusCode::InvalidArgument);
  }
}

// Cover the error/value combinations for copy and move assignment. Each inner
// scope must end with Counted::live empty (see Counted above).
TEST(StatusOrTest, AssignmentAcrossStates) {
  // error <- value: destination has no value yet, so it must not destroy its
  // unconstructed storage before taking the new value.
  {
    Counted::live.clear();
    {
      StatusOr<Counted> dst = Status::InvalidArgument("error");
      StatusOr<Counted> src{Counted{1}};
      dst = src;
      ASSERT_TRUE(dst.ok());
      EXPECT_EQ(dst.value().value, 1);
    }
    EXPECT_TRUE(Counted::live.empty());
  }
  // value <- error: destination holds a value that must be destroyed.
  {
    Counted::live.clear();
    {
      StatusOr<Counted> dst{Counted{2}};
      StatusOr<Counted> src = Status::InvalidArgument("error");
      dst = src;
      EXPECT_FALSE(dst.ok());
    }
    EXPECT_TRUE(Counted::live.empty());
  }
  // error <- value via move: the move counterpart of the first case, and the one
  // that regresses if move assignment sets status_ before constructing the value.
  {
    Counted::live.clear();
    {
      StatusOr<Counted> dst = Status::InvalidArgument("error");
      StatusOr<Counted> src{Counted{3}};
      dst = std::move(src);
      ASSERT_TRUE(dst.ok());
      EXPECT_EQ(dst.value().value, 3);
    }
    EXPECT_TRUE(Counted::live.empty());
  }
  // value <- value via move.
  {
    Counted::live.clear();
    {
      StatusOr<Counted> dst{Counted{4}};
      StatusOr<Counted> src{Counted{5}};
      dst = std::move(src);
      ASSERT_TRUE(dst.ok());
      EXPECT_EQ(dst.value().value, 5);
    }
    EXPECT_TRUE(Counted::live.empty());
  }
}

TEST(StatusOrTest, Swap) {
  // value <-> error: exercises both a live and an unconstructed operand.
  {
    Counted::live.clear();
    {
      StatusOr<Counted> a{Counted{1}};
      StatusOr<Counted> b = Status::InvalidArgument("error");
      a.swap(b);
      EXPECT_FALSE(a.ok());
      ASSERT_TRUE(b.ok());
      EXPECT_EQ(b.value().value, 1);
    }
    EXPECT_TRUE(Counted::live.empty());
  }
  // error <-> value: the mirror of the case above, covering the other
  // move-across branch (this side unconstructed, rhs holds the value).
  {
    Counted::live.clear();
    {
      StatusOr<Counted> a = Status::InvalidArgument("error");
      StatusOr<Counted> b{Counted{1}};
      a.swap(b);
      ASSERT_TRUE(a.ok());
      EXPECT_EQ(a.value().value, 1);
      EXPECT_FALSE(b.ok());
    }
    EXPECT_TRUE(Counted::live.empty());
  }
  // value <-> value (via the free swap function).
  {
    Counted::live.clear();
    {
      StatusOr<Counted> a{Counted{1}};
      StatusOr<Counted> b{Counted{2}};
      swap(a, b);
      ASSERT_TRUE(a.ok() && b.ok());
      EXPECT_EQ(a.value().value, 2);
      EXPECT_EQ(b.value().value, 1);
    }
    EXPECT_TRUE(Counted::live.empty());
  }
  // error <-> error.
  {
    StatusOr<Counted> a = Status::InvalidArgument("a");
    StatusOr<Counted> b = Status::NotFound("b");
    a.swap(b);
    EXPECT_EQ(a.code(), StatusCode::NotFound);
    EXPECT_EQ(b.code(), StatusCode::InvalidArgument);
  }
}

}  // namespace ray
