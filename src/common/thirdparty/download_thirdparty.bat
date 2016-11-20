@SetLocal
	@Echo Off
	@PushD "%~dp0"
		git                                                     submodule update --init --jobs="%NUMBER_OF_PROCESSORS%"
		@If Not Exist  "python\.git" git                        clone "https://github.com/austinsc/python.git"
		Call :GitApply "python"                                 "%CD%/patches/windows/python-pyconfig.patch"
	@PopD
@EndLocal
@GoTo :EOF

:GitApply <ChangeToFolder> <Patch>
	@REM Check if patch already applied by attempting to apply it in reverse; if not, then force-reapply it
	git -C "%~1" apply "%~2" -R --check 2> NUL || git -C "%~1" apply "%~2" --3way 2> NUL || git -C "%~1" reset --hard && git -C "%~1" apply "%~2" --3way
@GoTo :EOF
