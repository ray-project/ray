@SetLocal
	@Echo Off
	@PushD "%~dp0"
		git                                                     submodule update --init --jobs="%NUMBER_OF_PROCESSORS%"
		git -C "arrow"                                          submodule update --init --jobs="%NUMBER_OF_PROCESSORS%"
		git -C "grpc"                                           submodule update --init --jobs="%NUMBER_OF_PROCESSORS%" -- "third_party/nanopb" "third_party/protobuf"
		Call :GitApply "grpc"                                   "%CD%/patches/grpc-source.patch"
		Call :GitApply "grpc"                                   "%CD%/patches/windows/grpc-projects.patch"
		Call :GitApply "grpc/third_party/protobuf"              "%CD%/patches/windows/protobuf-projects.patch"
		Call :GitApply "arrow/cpp/thirdparty/flatbuffers"       "%CD%/patches/windows/flatbuffers-projects.patch"
		Call :GitApply "python"                                 "%CD%/patches/windows/python-pyconfig.patch"
	@PopD
@EndLocal
@GoTo :EOF

:GitApply <ChangeToFolder> <Patch>
	@REM Check if patch already applied by attempting to apply it in reverse; if not, then force-reapply it
	git -C "%~1" apply "%~2" -R --check 2> NUL || git -C "%~1" apply "%~2" --3way 2> NUL || git -C "%~1" reset --hard && git -C "%~1" apply "%~2" --3way
@GoTo :EOF
