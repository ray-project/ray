// Copyright 2020 The Ray Authors.
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

#include "ray/util/util.h"

#include <stdio.h>

#include <boost/asio/generic/basic_endpoint.hpp>
#include <boost/process/child.hpp>
#include <chrono>
#include <thread>

#include "gtest/gtest.h"
#include "ray/util/logging.h"
#include "ray/util/process.h"

using namespace std::chrono_literals;

static const char *argv0 = NULL;

namespace ray {

template <class T>
static std::string to_str(const T &obj, bool include_scheme) {
  return EndpointToUrl(obj, include_scheme);
}

TEST(UtilTest, UrlIpTcpParseTest) {
  ASSERT_EQ(to_str(ParseUrlEndpoint("tcp://[::1]:1/", 0), false), "[::1]:1");
  ASSERT_EQ(to_str(ParseUrlEndpoint("tcp://[::1]/", 0), false), "[::1]:0");
  ASSERT_EQ(to_str(ParseUrlEndpoint("tcp://[::1]:1", 0), false), "[::1]:1");
  ASSERT_EQ(to_str(ParseUrlEndpoint("tcp://[::1]", 0), false), "[::1]:0");
  ASSERT_EQ(to_str(ParseUrlEndpoint("tcp://127.0.0.1:1/", 0), false), "127.0.0.1:1");
  ASSERT_EQ(to_str(ParseUrlEndpoint("tcp://127.0.0.1/", 0), false), "127.0.0.1:0");
  ASSERT_EQ(to_str(ParseUrlEndpoint("tcp://127.0.0.1:1", 0), false), "127.0.0.1:1");
  ASSERT_EQ(to_str(ParseUrlEndpoint("tcp://127.0.0.1", 0), false), "127.0.0.1:0");
  ASSERT_EQ(to_str(ParseUrlEndpoint("[::1]:1/", 0), false), "[::1]:1");
  ASSERT_EQ(to_str(ParseUrlEndpoint("[::1]/", 0), false), "[::1]:0");
  ASSERT_EQ(to_str(ParseUrlEndpoint("[::1]:1", 0), false), "[::1]:1");
  ASSERT_EQ(to_str(ParseUrlEndpoint("[::1]", 0), false), "[::1]:0");
  ASSERT_EQ(to_str(ParseUrlEndpoint("127.0.0.1:1/", 0), false), "127.0.0.1:1");
  ASSERT_EQ(to_str(ParseUrlEndpoint("127.0.0.1/", 0), false), "127.0.0.1:0");
  ASSERT_EQ(to_str(ParseUrlEndpoint("127.0.0.1:1", 0), false), "127.0.0.1:1");
  ASSERT_EQ(to_str(ParseUrlEndpoint("127.0.0.1", 0), false), "127.0.0.1:0");
#ifndef _WIN32
  ASSERT_EQ(to_str(ParseUrlEndpoint("unix:///tmp/sock"), false), "/tmp/sock");
  ASSERT_EQ(to_str(ParseUrlEndpoint("/tmp/sock"), false), "/tmp/sock");
#endif
}

TEST(UtilTest, ParseCommandLineTest) {
  typedef std::vector<std::string> ArgList;
  CommandLineSyntax posix = CommandLineSyntax::POSIX, win32 = CommandLineSyntax::Windows,
                    all[] = {posix, win32};
  for (CommandLineSyntax syn : all) {
    ASSERT_EQ(ParseCommandLine(R"(aa)", syn), ArgList({R"(aa)"}));
    ASSERT_EQ(ParseCommandLine(R"(a )", syn), ArgList({R"(a)"}));
    ASSERT_EQ(ParseCommandLine(R"(\" )", syn), ArgList({R"(")"}));
    ASSERT_EQ(ParseCommandLine(R"(" a")", syn), ArgList({R"( a)"}));
    ASSERT_EQ(ParseCommandLine(R"("\\")", syn), ArgList({R"(\)"}));
    ASSERT_EQ(ParseCommandLine(/*R"("\"")"*/ "\"\\\"\"", syn), ArgList({R"(")"}));
    ASSERT_EQ(ParseCommandLine(R"(a" b c"d )", syn), ArgList({R"(a b cd)"}));
    ASSERT_EQ(ParseCommandLine(R"(\"a b)", syn), ArgList({R"("a)", R"(b)"}));
    ASSERT_EQ(ParseCommandLine(R"(| ! ^ # [)", syn), ArgList({"|", "!", "^", "#", "["}));
    ASSERT_EQ(ParseCommandLine(R"(; ? * $ &)", syn), ArgList({";", "?", "*", "$", "&"}));
    ASSERT_EQ(ParseCommandLine(R"(: ` < > ~)", syn), ArgList({":", "`", "<", ">", "~"}));
  }
  ASSERT_EQ(ParseCommandLine(R"( a)", posix), ArgList({R"(a)"}));
  ASSERT_EQ(ParseCommandLine(R"( a)", win32), ArgList({R"()", R"(a)"}));
  ASSERT_EQ(ParseCommandLine(R"(\ a)", posix), ArgList({R"( a)"}));
  ASSERT_EQ(ParseCommandLine(R"(\ a)", win32), ArgList({R"(\)", R"(a)"}));
  ASSERT_EQ(ParseCommandLine(R"(C:\ D)", posix), ArgList({R"(C: D)"}));
  ASSERT_EQ(ParseCommandLine(R"(C:\ D)", win32), ArgList({R"(C:\)", R"(D)"}));
  ASSERT_EQ(ParseCommandLine(R"(C:\\ D)", posix), ArgList({R"(C:\)", R"(D)"}));
  ASSERT_EQ(ParseCommandLine(R"(C:\\ D)", win32), ArgList({R"(C:\\)", R"(D)"}));
  ASSERT_EQ(ParseCommandLine(R"(C:\  D)", posix), ArgList({R"(C: )", R"(D)"}));
  ASSERT_EQ(ParseCommandLine(R"(C:\  D)", win32), ArgList({R"(C:\)", R"(D)"}));
  ASSERT_EQ(ParseCommandLine(R"(C:\\\  D)", posix), ArgList({R"(C:\ )", R"(D)"}));
  ASSERT_EQ(ParseCommandLine(R"(C:\\\  D)", win32), ArgList({R"(C:\\\)", R"(D)"}));
  ASSERT_EQ(ParseCommandLine(R"(\)", posix), ArgList({R"()"}));
  ASSERT_EQ(ParseCommandLine(R"(\)", win32), ArgList({R"(\)"}));
  ASSERT_EQ(ParseCommandLine(R"(\\a)", posix), ArgList({R"(\a)"}));
  ASSERT_EQ(ParseCommandLine(R"(\\a)", win32), ArgList({R"(\\a)"}));
  ASSERT_EQ(ParseCommandLine(R"(\\\a)", posix), ArgList({R"(\a)"}));
  ASSERT_EQ(ParseCommandLine(R"(\\\a)", win32), ArgList({R"(\\\a)"}));
  ASSERT_EQ(ParseCommandLine(R"(\\)", posix), ArgList({R"(\)"}));
  ASSERT_EQ(ParseCommandLine(R"(\\)", win32), ArgList({R"(\\)"}));
  ASSERT_EQ(ParseCommandLine(R"("\\a")", posix), ArgList({R"(\a)"}));
  ASSERT_EQ(ParseCommandLine(R"("\\a")", win32), ArgList({R"(\\a)"}));
  ASSERT_EQ(ParseCommandLine(R"("\\\a")", posix), ArgList({R"(\\a)"}));
  ASSERT_EQ(ParseCommandLine(R"("\\\a")", win32), ArgList({R"(\\\a)"}));
  ASSERT_EQ(ParseCommandLine(R"('a'' b')", posix), ArgList({R"(a b)"}));
  ASSERT_EQ(ParseCommandLine(R"('a'' b')", win32), ArgList({R"('a'')", R"(b')"}));
  ASSERT_EQ(ParseCommandLine(R"('a')", posix), ArgList({R"(a)"}));
  ASSERT_EQ(ParseCommandLine(R"('a')", win32), ArgList({R"('a')"}));
  ASSERT_EQ(ParseCommandLine(R"(x' a \b')", posix), ArgList({R"(x a \b)"}));
  ASSERT_EQ(ParseCommandLine(R"(x' a \b')", win32), ArgList({R"(x')", R"(a)", R"(\b')"}));
}

TEST(UtilTest, ExponentialBackOffTest) {
  auto exp = ExponentialBackOff(1, 2, 9);
  ASSERT_EQ(1, exp.Next());
  ASSERT_EQ(2, exp.Next());
  ASSERT_EQ(4, exp.Next());
  ASSERT_EQ(8, exp.Next());
  ASSERT_EQ(9, exp.Next());
  ASSERT_EQ(9, exp.Next());
  exp.Reset();
  ASSERT_EQ(1, exp.Next());
  ASSERT_EQ(2, exp.Next());
  ASSERT_EQ(4, exp.Next());
  ASSERT_EQ(8, exp.Next());
  ASSERT_EQ(9, exp.Next());
  ASSERT_EQ(9, exp.Next());
}

TEST(UtilTest, ParseURLTest) {
  const std::string url = "http://abc?num_objects=9&offset=8388878&size=8388878";
  auto parsed_url = *ParseURL(url);
  ASSERT_EQ(parsed_url["url"], "http://abc");
  ASSERT_EQ(parsed_url["num_objects"], "9");
  ASSERT_EQ(parsed_url["offset"], "8388878");
  ASSERT_EQ(parsed_url["size"], "8388878");
}

TEST(UtilTest, CreateCommandLineTest) {
  typedef std::vector<std::string> ArgList;
  CommandLineSyntax posix = CommandLineSyntax::POSIX, win32 = CommandLineSyntax::Windows,
                    all[] = {posix, win32};
  std::vector<ArgList> test_cases({
      ArgList({R"(a)"}),
      ArgList({R"(a b)"}),
      ArgList({R"(")"}),
      ArgList({R"(')"}),
      ArgList({R"(\)"}),
      ArgList({R"(/)"}),
      ArgList({R"(#)"}),
      ArgList({R"($)"}),
      ArgList({R"(!)"}),
      ArgList({R"(@)"}),
      ArgList({R"(`)"}),
      ArgList({R"(&)"}),
      ArgList({R"(|)"}),
      ArgList({R"(a")", R"('x)", R"(?'"{)", R"(]))", R"(!)", R"(~`\)"}),
  });
  for (CommandLineSyntax syn : all) {
    for (const ArgList &arglist : test_cases) {
      ASSERT_EQ(ParseCommandLine(CreateCommandLine(arglist, syn), syn), arglist);
      std::string cmdline = CreateCommandLine(arglist, syn);
      std::string buf((2 + cmdline.size()) * 6, '\0');
      std::string test_command = std::string(argv0);
      test_command = "\"" + test_command + "\"";
#ifdef _WIN32
      test_command = "\"" + test_command;
#endif
      test_command += " --println " + cmdline;
#ifdef _WIN32
      test_command = test_command + "\"";
#endif
      FILE *proc;
#ifdef _WIN32
      proc = syn == win32 ? _popen(test_command.c_str(), "r") : NULL;
#else
      proc = syn == posix ? popen(test_command.c_str(), "r") : NULL;
#endif
      if (proc) {
        std::vector<std::string> lines;
        while (fgets(&*buf.begin(), static_cast<int>(buf.size()), proc)) {
          lines.push_back(buf.substr(0, buf.find_first_of(std::string({'\0', '\n'}))));
        }
        ASSERT_EQ(lines, arglist);
#ifdef _WIN32
        _pclose(proc);
#else
        pclose(proc);
#endif
      }
    }
  }
}

TEST(UtilTest, IsProcessAlive) {
  namespace bp = boost::process;
  bp::child c("bash");
  auto pid = c.id();
  c.join();
  for (int i = 0; i < 5; ++i) {
    if (IsProcessAlive(pid)) {
      std::this_thread::sleep_for(1s);
    } else {
      break;
    }
  }
  RAY_CHECK(!IsProcessAlive(pid));
}

}  // namespace ray

int main(int argc, char **argv) {
  argv0 = argv[0];
  int result = 0;
  if (argc > 1 && strcmp(argv[1], "--println") == 0) {
    // If we're given this special command, emit each argument on a new line
    for (int i = 2; i < argc; ++i) {
      fprintf(stdout, "%s\n", argv[i]);
    }
  } else {
    ::testing::InitGoogleTest(&argc, argv);
    result = RUN_ALL_TESTS();
  }
  return result;
}
