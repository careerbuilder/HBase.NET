//Copyright 2012 CareerBuilder, LLC. - http://www.careerbuilder.com

//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Hbase;
using Rhino.Mocks;
using Thrift;
using System.Net.Sockets;
using System.IO;
using System.Threading;
using Thrift.Transport;

namespace TestHBase.NET.TestDummies
{
    internal class TestHBaseConnection : IHBaseConnection
    {
        public HBaseErrorContinuity ErrorContinuity { get; set; }
        public HBaseErrorType ErrorType { get; set; }

        public TestHBaseConnection(HBaseErrorContinuity ec = HBaseErrorContinuity.Never, HBaseErrorType et = HBaseErrorType.Application)
        {
            this.ErrorContinuity = ec;
            this.ErrorType = et;

            this._Client = new TestHBaseIface(ec, et);
        }

        public string Host
        {
            get { return "localhost"; }
        }

        public int Port
        {
            get { return 1010; }
        }

        public int Timeout
        {
            get { return 1000; }
        }

        public int BufferSize
        {
            get { return 102400; }
        }

        private Hbase.Hbase.Iface _Client;
        public Hbase.Hbase.Iface GetClient(int Timeout)
        {
            return _Client;
        }

        public bool IsAlive()
        {
            return true;
        }

        public void Reset()
        { }

        public void Dispose()
        { }
    }
}
