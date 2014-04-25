﻿//Copyright 2012 CareerBuilder, LLC. - http://www.careerbuilder.com

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

namespace TestHBase.NET.TestDummies
{
    internal class TestPooledHBaseClient : IPooledHBaseClient
    {
        private string _Host = "TestDummy";

        protected Hbase.Hbase.Iface _Client { get; set; }

        public TestPooledHBaseClient(Hbase.Hbase.Iface client)
        {
            this._Client = client;
        }

        public void Reset()
        { }

        public void Execute(HBaseOperation op)
        {
            op.Invoke(this._Client);
        }

        public T Execute<T>(Func<Hbase.Hbase.Iface, T> op)
        {
            return op.Invoke(this._Client);
        }

        public event PooledHBaseClientDisposeHandler Disposing;

        public int Timeout
        {
            get;
            set;
        }

        public string Host
        {
            get
            {
                return _Host;
            }
        }

        public void Dispose()
        {
            if (this.Disposing != null) this.Disposing(null);
        }
    }
}
