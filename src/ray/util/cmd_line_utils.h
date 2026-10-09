// Copyright 2025 The Ray Authors.
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

#include <string>
#include <vector>

enum class CommandLineSyntax { System, POSIX, Windows };

/// A helper function to parse command-line arguments in a platform-compatible manner.
///
/// \param cmdline The command-line to split.
///
/// \return The command-line arguments, after processing any escape sequences.
std::vector<std::string> ParseCommandLine(
    const std::string &cmdline, CommandLineSyntax syntax = CommandLineSyntax::System);

/// A helper function to combine command-line arguments in a platform-compatible manner.
/// The result of this function is intended to be suitable for the shell used by popen().
///
/// \param cmdline The command-line arguments to combine.
///
/// \return The command-line string, including any necessary escape sequences.
std::string CreateCommandLine(const std::vector<std::string> &args,
                              CommandLineSyntax syntax = CommandLineSyntax::System);
