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

namespace TestHBase.NET.TestDummies
{
    class TestHBaseClientPool : IHBaseClientPool
    {
        protected IPooledHBaseClient Client { get; set; }
        public TestHBaseClientPool(IPooledHBaseClient client)
        {
            this.Client = client;
        }

        public int AvailableClients
        {
            get { throw new NotImplementedException(); }
        }

        public int BlockedRequests
        {
            get { throw new NotImplementedException(); }
        }

        public int TotalClients
        {
            get { throw new NotImplementedException(); }
        }

        public void Execute(HBaseClientOperation op)
        {
            op.Invoke(this.Client);
        }

        public T Execute<T>(Func<IPooledHBaseClient, T> op)
        {
            return op.Invoke(this.Client);
        }

        public void PollFaultyClients()
        { }

        public void Dispose()
        {
            this.Client.Dispose();
        }
    }
}
