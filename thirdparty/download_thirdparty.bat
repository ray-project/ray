@SetLocal
	@Echo Off
	@PushD "%~dp0"
		If Not Exist "arrow\.git"  git clone  --recursive --branch windows_with_submodules "https://github.com/pcmoritz/arrow.git"
		If Not Exist "python\.git" git clone                                               "https://github.com/austinsc/python.git"
		git                                                     submodule update --init
		git -C "arrow"                                          submodule update --init
		git -C "grpc"                                           submodule update --init "third_party/protobuf"
		git -C "grpc"                                           submodule update --init "third_party/nanopb"
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
	git -C "%~1" apply --3way "%~2" -R --check 2> NUL || git -C "%~1" reset --hard && git -C "%~1" apply --3way "%~2"
@GoTo :EOF
