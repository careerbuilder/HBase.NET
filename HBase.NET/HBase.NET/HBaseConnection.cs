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
using Thrift.Transport;
using Thrift.Protocol;

namespace Hbase
{
    /// <summary>
    /// Holds the information for an HBase Thrift Connection
    /// </summary>
    /// <remarks>
    /// Errors thrown:
    /// Errors thrown by the Thrift generated client are clasified as follows:
    /// -- SocketException (with SocketErrorCode = Timeout)
    ///     The server took too long to respond. This may be caused by the server being down or innaccesible.
    ///     These operations should be retried on a different connection.
    /// -- IOException wrapping a SocketException (with SocketErrorCode)
    ///     These errors are most likely generated by a client side inconsistency in the connection.
    ///     These operations should be retried on the same connection after being reset.
    /// -- TApplicationException
    ///     The error comes directly from the HBase application layer.
    ///     These operations should never be retried.
    /// -- TTransportException
    ///     This error is reported by the Thrift TTransport. It is assumed that it only relates to transport 
    ///     (network) errors.
    ///     These operations should be retried.
    /// </remarks>
    internal class HBaseConnection
        : IHBaseConnection
    {

        public string Host { get { return this._Host; } }
        public int Port { get { return this._Port; } }
        public int BufferSize { get { return this._BufferSize; } }

        private string _Host;
        private int _Port;
        private int _BufferSize;
        private Hbase.Iface _Client;
        private TBufferedTransport _Transport;
        private TSocket _Socket;

        internal HBaseConnection(Hbase.Iface Client, TBufferedTransport Transport)
        {
            this._Client = Client;
            this._Transport = Transport;
        }

        public HBaseConnection(string Host, int Port, int BufferSize)
        {
            this._Host = Host;
            this._Port = Port;
            this._BufferSize = BufferSize;
        }

        public Hbase.Iface GetClient(int Timeout)
        {
            if (this._Client == null)
            {
                this._Socket = new TSocket(this.Host, this.Port) { Timeout = Timeout };
                this._Transport = new TBufferedTransport( this._Socket, this.BufferSize);
                this._Client = new Hbase.Client(new TBinaryProtocol(this._Transport));
                this._Transport.Open();
            }

            if ((object)_Socket != null)
            {
                _Socket.Timeout = Timeout;
            }

            return this._Client;
        }

        public void Reset()
        {
            if ((object)_Transport != null && _Transport.IsOpen)
            {
                _Transport.Close();
            }

            this._Transport = null;
            this._Client = null;
        }

        public bool IsAlive()
        {
            return this._Client != null && this._Transport.IsOpen;
        }

        #region IDisposable Support
        private bool Disposed;

        protected virtual void Dispose(bool Disposing)
        {
            if (!Disposed && Disposing)
            {
                if ((object)_Transport != null)
                {
                    _Transport.Close();
                }
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

    internal interface IHBaseConnection
        : IDisposable
    {
        string Host { get; }
        int Port { get; }
        int BufferSize { get; }

        Hbase.Iface GetClient(int Timeout);

        bool IsAlive();
        void Reset();
    }
}