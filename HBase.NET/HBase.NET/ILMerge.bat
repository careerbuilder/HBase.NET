CD %~dp0
IF NOT EXIST ..\bin MKDIR ..\bin
ILMerge.exe %1 ..\dll\Thrift.dll dll\Castle.Core.dll dll\log4net.dll /targetplatform:v4 /ndebug /internalize /out:..\bin\HBase.NET.dll
COPY "..\Open Source Licenses.txt" ..\bin