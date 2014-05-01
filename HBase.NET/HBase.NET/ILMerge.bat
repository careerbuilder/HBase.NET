IF NOT EXIST "%~dp0\..\bin" MKDIR "%~dp0\..\bin"
ILMerge.exe "%~dp0\%1" "%~dp0\..\dll\Thrift.dll" "%~dp0\dll\Castle.Core.dll" "%~dp0\dll\log4net.dll" /targetplatform:v4 /ndebug /internalize "/out:%~dp0\..\bin\HBase.NET.dll"
COPY "%~dp0\..\Open Source Licenses.txt" "%~dp0\..\bin"