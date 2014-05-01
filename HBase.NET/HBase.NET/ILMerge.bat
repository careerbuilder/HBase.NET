:: Copyright 2012 CareerBuilder, LLC. - http://www.careerbuilder.com

:: Licensed under the Apache License, Version 2.0 (the "License");
:: you may not use this file except in compliance with the License.
:: You may obtain a copy of the License at

::   http://www.apache.org/licenses/LICENSE-2.0

:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.

IF NOT EXIST "%~dp0\..\bin" MKDIR "%~dp0\..\bin"
ILMerge.exe "%~dp0\%1" "%~dp0\..\dll\Thrift.dll" "%~dp0\dll\Castle.Core.dll" "%~dp0\dll\log4net.dll" /targetplatform:v4 /ndebug /internalize "/out:%~dp0\..\bin\HBase.NET.dll"
COPY "%~dp0\..\Open Source Licenses.txt" "%~dp0\..\bin"