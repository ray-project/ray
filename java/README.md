### Instructions on ray-java test

1. Install necessary executables
- `java` and `javac` is needed to run ray-java tests, and users need to make sure they're accessible in `$PATH`
- You could check whether they're installed by `which java` and `which javac`
- Install `java` with `sudo apt install openjdk-11-jre -y`
- Install `javac` with `sudo apt install openjdk-11-jdk -y`
- java-11 is the version we use on CI

2. Run java test with bazel
```sh
# To run ray tests.
bazel test //java:all_tests --test_output=streamed
# To run custom tests.
bazel test //java:custom_test --test_output=streamed
```
