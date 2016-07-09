@SetLocal
	@PushD "%~dp0"
		git                                                        submodule update --init
		@If Not Exist "grpc\.git"   git                            clone "https://github.com/grpc/grpc"
		@If Not Exist "arrow\.git"  git                            clone "https://github.com/pcmoritz/arrow.git" --branch windows_with_submodules --recursive
		@If Not Exist "arrow\cpp\thirdparty\flatbuffers\.git"  git clone "https://github.com/google/flatbuffers.git" "arrow/cpp/thirdparty/flatbuffers"
		@If Not Exist "numbuf\.git" git                            clone "https://github.com/amplab/numbuf.git" --branch win
		@If Not Exist "python\.git" git                            clone "https://github.com/austinsc/python.git"
		git -C "grpc"                                              submodule update --init "third_party/protobuf"
		git -C "grpc"                                              submodule update --init "third_party/nanopb"
		git -C "grpc"                                              submodule update --init "third_party/zlib"
		git -C "grpc"                                              apply --index --3way "%~dp0patches/grpc-source.patch"
		git -C "grpc"                                              apply --index --3way "%~dp0patches/windows/grpc-projects.patch"
		git -C "grpc/third_party/protobuf"                         apply --index --3way "%~dp0patches/windows/protobuf-projects.patch"
		git -C "arrow/cpp/thirdparty/flatbuffers"                  apply --index --3way "%~dp0patches/windows/flatbuffers-projects.patch"
		git -C "python"                                            apply --index --3way "%~dp0patches/windows/python-pyconfig.patch"
	@PopD
@EndLocal
