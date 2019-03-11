# Checkstyle integration

## What is Checkstyle

>[checkstyle](http://checkstyle.sourceforge.net/) is a development tool to 
help programmers write Java code that adheres to a coding standard. 
It automates the process of checking Java code to spare humans of this 
boring (but important) task. This makes it ideal for projects that want to 
enforce a coding standard.

## Integrating with `bazel`

To apply `checkstyle` check to Java code built with `bazel`, 
we use the `checkstyle_test` rule.

Example:

`WORKSPACE`:
```
load("//tools/checkstyle:checkstyle.bzl", "checkstyle_dependencies")
checkstyle_dependencies()
```

`BUILD`:

```
load("//tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_library(
    name = "lib",
    srcs = ["Lib.java"],
)

java_library(
    name = "ignored_lib",
    srcs = ["Lib.java"],
    tags = ["checkstyle_ignore"]
)

checkstyle_test(
    name = "lib-checkstyle",
    allow_failure = 1,
    target = ":lib",
)

```

Sample code illustrates several points:
* To apply Checkstyle to Java target named `lib`, create a `checkstyle_test` 
named `lib-checkstyle` (with a dash). Such naming convention is enforced at rule level.
* `checkstyle_test` **is** a test in Bazel context, which means you can do 
`bazel test //:lib-checkstyle` and get result of that inspection.
* To perform initial integration, `allow_failure` attribute can be temporarily set,
which will _still_ perform Checkstyle test, but will succeed (as Bazel test) 
independently of check's result.
* Targets that do not need to be checked (e.g. generated code), such as `ignored_lib`
can be ignored by assigning `tags = ["checkstyle_ignore"]`
* Checkstyle test **completeness** (that is, all Java code that _needs_ checking 
**is** being checked) is enforced via the shell script named `check-for-missing-targets.sh` 
ran by CI. If you omit **both** `checkstyle_test` target and `checkstyle_ignore` tag, the script
will exit with non-zero code and print out non-covered targets.

## Configuring Checkstyle

Checkstyle is configured by an XML file. By default, current rule implementation uses
`//tools/checkstyle:config.xml` which is based on Google Java style 
with added checks of license header 
(via [`RegexpHeader`](http://checkstyle.sourceforge.net/config_header.html#RegexpHeader) 
and `//tools/checkstyle:license-header.txt`) and package header 
(via [`PackageName`](http://checkstyle.sourceforge.net/config_naming.html#PackageName))
