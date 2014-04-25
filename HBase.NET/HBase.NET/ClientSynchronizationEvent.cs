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
using System.Threading;

namespace Hbase
{
    internal class ClientSynchronizationEvent
    {
        private Object _SyncObject;
        private IHBaseConnection _Connection;
        private bool _ThrowException = false;
        private bool _ClientIsWaiting = false;
        private readonly string _Parent;

        public ClientSynchronizationEvent(string Parent)
        {
            _SyncObject = new object();
            _Parent = Parent;
        }

        public bool Wait(int Timeout)
        {
            lock (_SyncObject)
            {
                _ClientIsWaiting = true;
                bool ReturnValue = Monitor.Wait(_SyncObject, Timeout);
                _ClientIsWaiting = false;

                if (_ThrowException)
                {
                    throw new ObjectDisposedException(_Parent, "The parent object has been disposed. All waiting threads are immediately unsuspended");
                }

                return ReturnValue;
            }
        }

        public void ThrowExceptionOnSuspendedThread()
        {
            lock (_SyncObject)
            {
                _ThrowException = true;
                Monitor.Pulse(_SyncObject);
            }
        }

        /// <summary>
        /// Sets the Connection object and wakes up the waiting Client Thread
        /// </summary>
        /// <param name="Connection"></param>
        /// <returns>True if the Client was successfully awaken and the Connection was assigned. False if
        /// the connection was not successfully assigned (most likely because the Client was no longer waiting).</returns>
        public bool Set(IHBaseConnection Connection)
        {
            lock (_SyncObject)
            {
                if (!this._ClientIsWaiting) return false;
                _Connection = Connection;
                Monitor.Pulse(_SyncObject);
                return true;
            }
        }

        public IHBaseConnection GetNextClient()
        {
            lock (_SyncObject)
            {
                return _Connection;
            }
        }
    }
}