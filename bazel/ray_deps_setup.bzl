load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def ray_deps_setup(
        local_bazel_project_path,
        local_glog_path,
        local_arrow_path
        ):
  RULES_JVM_EXTERNAL_TAG = "2.0.1"
  RULES_JVM_EXTERNAL_SHA = "55e8d3951647ae3dffde22b4f7f8dee11b3f70f3f89424713debd7076197eaca"
  http_archive(
      name = "rules_jvm_external",
      strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
      sha256 = RULES_JVM_EXTERNAL_SHA,
      url = "http://aivolvo.cn-hangzhou.oss.jiuzhou.cloud.aliyun-inc.com/raylib/rules_jvm_external/rules_jvm_external-%s.zip" % RULES_JVM_EXTERNAL_TAG,
  )

  http_archive(
      name = "bazel_common",
      strip_prefix = "bazel-common-f1115e0f777f08c3cdb115526c4e663005bec69b",
      url = "http://aivolvo.cn-hangzhou.oss.jiuzhou.cloud.aliyun-inc.com/raylib/bazel-common/bazel-common-f1115e0f777f08c3cdb115526c4e663005bec69b.zip",
  )

  BAZEL_SKYLIB_TAG = "0.6.0"
  http_archive(
      name = "bazel_skylib",
      strip_prefix = "bazel-skylib-%s" % BAZEL_SKYLIB_TAG,
      url = "http://aivolvo.cn-hangzhou.oss.jiuzhou.cloud.aliyun-inc.com/raylib/bazel-skylib/bazel-skylib-%s.tar.gz" % BAZEL_SKYLIB_TAG,
  )

  git_repository(
      name = "com_github_checkstyle_java",
      commit = "85f37871ca03b9d3fee63c69c8107f167e24e77b",
      remote = "https://github.com/ruifangChen/checkstyle_java",
  )

  native.local_repository(
      name = "com_github_nelhage_rules_boost",
      path = local_bazel_project_path + "/boost",
  )

  native.local_repository(
      name = "flatbuffers",
      path = local_bazel_project_path + "/flatbuffers",
  )

  http_archive(
      name = "com_google_googletest",
      strip_prefix = "googletest",
      sha256 = "1bd0040f1c76cb050815698cb9e76afda2aa470e457719f85a83510106d99d81",
      urls = ["http://aivolvo.cn-hangzhou.oss.jiuzhou.cloud.aliyun-inc.com/raylib/googletest-3306848f697568aacf4bcca330f6bdd5ce671899.tar.gz"],
  )

  git_repository(
      name = "com_github_gflags_gflags",
      remote = "https://github.com/gflags/gflags.git",
      tag = "v2.2.2",
  )

  native.local_repository(
      name = "com_github_google_glog",
      path = local_glog_path,
  )

  native.new_local_repository(
      name = "plasma",
      path = local_arrow_path,
      build_file = "@com_github_ray_project_ray//bazel:BUILD.plasma",
  )

  native.local_repository(
      name = "prometheus_cpp",
      path = local_bazel_project_path + "/prometheus_cpp",
  )

  native.local_repository(
      name = "kmonitor_client",
      path = local_bazel_project_path + "/kmonitor_client",
  )

  http_archive(
      name = "cython",
      build_file = "@com_github_ray_project_ray//bazel:BUILD.cython",
      urls = ["http://aivolvo.cn-hangzhou.oss.jiuzhou.cloud.aliyun-inc.com/raylib/cython-0.29.3.tar.gz"],
      sha256 = "4a33fd40c28e67413817f814adadfe2047a66ac3dcc81ff81651a54a03d6cbd0",
      strip_prefix = "cython-0.29.3",
  )
