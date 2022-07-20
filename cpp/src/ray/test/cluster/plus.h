// Copyright 2021 The Ray Authors.
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

#pragma once

#include <ray/api.h>

/// general function of user code
int Return1();
int Plus1(int x);
int Plus(int x, int y);
void ThrowTask();

std::array<int, 100000> ReturnLargeArray(std::array<int, 100000> x);
std::string Echo(std::string str);
std::map<int, std::string> GetMap(std::map<int, std::string> map);
std::array<std::string, 2> GetArray(std::array<std::string, 2> array);
std::vector<std::string> GetList(std::vector<std::string> list);
std::tuple<int, std::string> GetTuple(std::tuple<int, std::string> tp);

std::string GetNamespaceInTask();

struct Student {
  std::string name;
  int age;
  MSGPACK_DEFINE(name, age);
};

Student GetStudent(Student student);
std::map<int, Student> GetStudents(std::map<int, Student> students);
