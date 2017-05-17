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
using System.IO;
using System.Net.Sockets;
using Thrift.Transport;

namespace Hbase
{
    internal class PooledHBaseClient
        : IPooledHBaseClient
    {
        private IHBaseConnection _Connection;
        private int _Timeout;

        public event PooledHBaseClientDisposeHandler Disposing;

        public PooledHBaseClient(IHBaseConnection Connection, int Timeout)
        {
            _Connection = Connection;
            _Timeout = Timeout;
        }

        public string Host
        {
            get
            {
                return this._Connection.Host;
            }
        }

        public void Reset()
        {
            this._Connection.Reset();
        }

        public void Execute(HBaseOperation op)
        {
            DateTime TimeStamp = DateTime.Now;

            try
            {
                op.Invoke(this._Connection.GetClient(ConnectionTimeoutManager.GetRemainingTimeout(_Timeout, TimeStamp)));
            }
            catch (IOException ioex)
            {
                if (ioex.InnerException is SocketException)
                {
                    // Let's reset the Connection and try again
                    this._Connection.Reset();
                    op.Invoke(this._Connection.GetClient(ConnectionTimeoutManager.GetRemainingTimeout(_Timeout, TimeStamp)));
                }
                else
                {
                    throw;
                }
            }
            catch (TTransportException)
            {
                // Let's reset the Connection and try again
                this._Connection.Reset();
                op.Invoke(this._Connection.GetClient(ConnectionTimeoutManager.GetRemainingTimeout(_Timeout, TimeStamp)));
            }
        }

        public T Execute<T>(Func<Hbase.Iface, T> op)
        {
            DateTime TimeStamp = DateTime.Now;

            try
            {
                return op.Invoke(this._Connection.GetClient(ConnectionTimeoutManager.GetRemainingTimeout(_Timeout, TimeStamp)));
            }
            catch (IOException ioex)
            {
                if (ioex.InnerException is SocketException)
                {
                    // Let's reset the Connection and try again
                    this._Connection.Reset();
                    return op.Invoke(this._Connection.GetClient(ConnectionTimeoutManager.GetRemainingTimeout(_Timeout, TimeStamp)));
                }
                else
                {
                    throw;
                }
            }
            catch (TTransportException)
            {
                // Let's reset the Connection and try again
                this._Connection.Reset();
                return op.Invoke(this._Connection.GetClient(ConnectionTimeoutManager.GetRemainingTimeout(_Timeout, TimeStamp)));
            }
        }

        public int Timeout
        {
            get
            {
                return _Timeout;
            }
            set
            {
                _Timeout = value;
            }
        }

        #region IDisposable Support
        private bool Disposed;

        protected virtual void Dispose(bool Disposing)
        {
            if (!Disposed && Disposing)
            {
                if (this.Disposing != null) this.Disposing(this._Connection);
            }

            Disposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }

    internal delegate void HBaseOperation(Hbase.Iface c);
    internal delegate void PooledHBaseClientDisposeHandler(IHBaseConnection union);

    internal interface IPooledHBaseClient
        : IDisposable
    {
        void Reset(); 
        void Execute(HBaseOperation op);
        T Execute<T>(Func<Hbase.Iface, T> op);

        int Timeout
        {
            get;
            set;
        }

        string Host
        {
            get;
        }

        event PooledHBaseClientDisposeHandler Disposing;
    }
}