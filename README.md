#### Project Description

HBase.NET is a powerful, object-oriented HBase client for .NET which supports ORM-style HBase reads and writes. HBase.NET uses reflection to serialize POCOs and store them in HBase appropriately. The HBase client will preform it's own connection pooling internally, and could optionally also only use a single connection to the thrift server.

#### License

Copyright 2012 CareerBuilder, LLC. - <http://www.careerbuilder.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   <http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

#### Contributors

Thomas Rega  
Mauricio Morales  
Geoffrey Jacoby  

See Acknowledgements for additional contributions.

#### Acknowledgements

This project uses apache thrift's thrift-0.11.0.exe in unison with hbase.thrift to autogenerate the low-level hbase thrift client. (see: <http://archive.apache.org/dist/thrift/0.11.0/>; <http://svn.apache.org/viewvc/hbase/trunk/hbase-thrift/src/main/resources/org/apache/hadoop/hbase/thrift/>)

This project uses apache thrift's thrift.dll to communicate with HBase. (see: <http://wiki.apache.org/thrift/ThriftUsageCSharp>)

This project uses Castle Dynamic Proxy (see: <http://www.castleproject.org/projects/dynamicproxy/>) to construct and intercept interfaces. Without Castle, returning proxies of interfaces would not have been possible.

This project heavily uses Rhino Mocks (see: <http://hibernatingrhinos.com/open-source/rhino-mocks>) in order to easily test the underlying functionality of the high-level client.

All open source licenses have been included with their respective files.