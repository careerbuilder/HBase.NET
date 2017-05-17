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
using Thrift.Transport;
using Thrift.Protocol;
using Hbase.Iterators;
using Hbase.Iterators.RoundRobin;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Net.Sockets;
using Thrift;
using System.Runtime.Serialization;
using System.Reflection;
using Hbase.StaticInternals;

namespace Hbase
{
    internal class HBaseClientPool
        : IHBaseClientPool
    {
        private const int DEFAULTTIMEOUT = 20000;
        private const BindingFlags CONSTRUCTORATTRIBUTES = BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.CreateInstance | BindingFlags.Instance;

        private readonly ConcurrentQueue<IHBaseConnection> _AvailableConnections = new ConcurrentQueue<IHBaseConnection>();
        private readonly ConcurrentQueue<IPooledHBaseClient> _FaultyClients = new ConcurrentQueue<IPooledHBaseClient>();
        private readonly int _Timeout;
        private IIterator<HBaseHost> _Hosts;
        private readonly int _BufferSize;
        private string _CanaryTable = string.Empty;
        private ConcurrentQueue<ClientSynchronizationEvent> _ClientSynchronization = new ConcurrentQueue<ClientSynchronizationEvent>();
        protected Timer _FaultyClientsTimer;

        private int _OriginalClients;
        public int OriginalClients { get { return this._OriginalClients; } }


        protected HBaseClientPool()
        {
            this._FaultyClientsTimer = new Timer(new TimerCallback(PollFaultyClients), -1, 300000 /* 5 * 60 * 1000 */, 300000 /* 5 * 60 * 1000 */);
        }

        internal HBaseClientPool(IEnumerable<IHBaseConnection> ClientTransports, int timeout = DEFAULTTIMEOUT)
            : this()
        {
            _Timeout = timeout;

            foreach (IHBaseConnection Conn in ClientTransports)
            {
                _AvailableConnections.Enqueue(Conn);
            }
            this._OriginalClients = _AvailableConnections.Count;
        }

        public HBaseClientPool(IEnumerable<HBaseHost> hosts, int BufferSize, int NumConnections, int Timeout, string CanaryTable, bool UseCompactProtocol, bool UseFramedTransport)
            : this()
        {
            _Timeout = Timeout;
            _Hosts = new RoundRobinIterator<HBaseHost>(hosts);
            _BufferSize = BufferSize;
            _CanaryTable = CanaryTable;

            for (int i = 0; i < NumConnections; ++i)
            {
                var nextHost = this._Hosts.Next();
                _AvailableConnections.Enqueue(new HBaseConnection(nextHost.Host, nextHost.Port, _BufferSize, UseCompactProtocol, UseFramedTransport));
            }
            this._OriginalClients = _AvailableConnections.Count;
        }

        public int AvailableClients
        {
            get { return _AvailableConnections.Count; }
        }

        public int BlockedRequests
        {
            get { return _ClientSynchronization.Count; }
        }

        /// <summary>
        /// This implementation is potentially unsafe. If a Faulty Clients Poll is currently running,
        /// the _FaultyClients.Count count may not reflect/consider the in-memory stillFailingClients. 
        /// </summary>
        public int TotalClients
        {
            get { return _AvailableConnections.Count + _FaultyClients.Count; }
        }

        protected void ReturnClient(IHBaseConnection Conn)
        {
            try
            {
                if ((object)_ClientSynchronization == null || this.Disposing)
                {
                    Conn.Dispose();
                }
                else
                {
                    bool ReturnedConnection = false;

                    ClientSynchronizationEvent clientSync;
                    while (!ReturnedConnection && _ClientSynchronization.TryDequeue(out clientSync))
                    {
                        ReturnedConnection = clientSync.Set(Conn);
                    }

                    if (!ReturnedConnection)
                    {
                        _AvailableConnections.Enqueue(Conn);
                    }
                }
            }
            catch
            {
                _AvailableConnections.Enqueue(Conn);

                throw;
            }
        }

        protected IPooledHBaseClient GetNextAvailableClient(int Timeout)
        {
            DateTime TimeStamp = DateTime.Now;

            if (this.Disposing)
            {
                throw new ObjectDisposedException("HBaseClientPool", "This pool is being disposed.");
            }

            IHBaseConnection Conn = null;
            ClientSynchronizationEvent SynchronizationEvent = null;

            try
            {
                if (!_AvailableConnections.TryDequeue(out Conn))
                {
                    // try and recover connections
                    this.PollFaultyClients(ConnectionTimeoutManager.GetRemainingTimeout(_Timeout, TimeStamp));

                    if (!_AvailableConnections.TryDequeue(out Conn))
                    {
                        if (_FaultyClients.Count > 0)
                        {
                            Logger.Log.Warn("Faulty clients exist which are potentially starving the available client connection pool. Check Hbase services.");
                        }
                        else
                        {
                            Logger.Log.Warn("No connections available! It is strongly recommended that you increase connection pool size!");
                        }

                        SynchronizationEvent = new ClientSynchronizationEvent(this.GetType().Name);
                        _ClientSynchronization.Enqueue(SynchronizationEvent);
                    }
                }

                if ((object)SynchronizationEvent != null)
                {
                    int SynchronizationTimeout = ConnectionTimeoutManager.GetRemainingTimeout(Timeout, TimeStamp);

                    if (SynchronizationEvent.Wait(SynchronizationTimeout))
                    {
                        Conn = SynchronizationEvent.GetNextClient();
                    }
                    else
                    {
                        throw new TimeoutException("Failed to get connection after " + _Timeout.ToString() + "ms.");
                    }
                }

                if (!Conn.IsAlive())
                {
                    Conn.Reset();
                }

                int PooledClientTimeout = ConnectionTimeoutManager.GetRemainingTimeout(Timeout, TimeStamp);
                var pooledClient = new PooledHBaseClient(Conn, PooledClientTimeout);
                pooledClient.Disposing += this.ReturnClient;
                return pooledClient;
            }
            catch (ObjectDisposedException)
            {
                Logger.Log.Info("HBaseClientPool is being disposed. Exiting gracefully.");
                throw;
            }
            catch (TimeoutException)
            {
                if ((object)Conn != null)
                {
                    ReturnClient(Conn);
                }

                throw;
            }
            catch (Exception e)
            {
                if ((object)Conn == null)
                {
                    throw new NullReferenceException("The connection is nothing?!?", e);
                }
                else
                {
                    ReturnClient(Conn);
                }

                throw;
            }
        }

        public void Execute(HBaseClientOperation op)
        {
            try
            {
                DateTime TimeStamp = DateTime.Now;
                int clientRetries = 1;
                IPooledHBaseClient client = this.GetNextAvailableClient(ConnectionTimeoutManager.GetRemainingTimeout(_Timeout, TimeStamp));
                do
                {
                    try
                    {
                        try
                        {
                            op.Invoke(client);
                            client.Dispose();
                            client = null;
                        }
                        catch (IOException ioex)
                        {
                            if (ioex.InnerException is SocketException)
                            {
                                // if we have retried too many times, let's discontinue the execution
                                if (clientRetries < this.OriginalClients)
                                {
                                    // keep trying till n-1, n = Number of Connections in the Pool
                                    this._FaultyClients.Enqueue(client);
                                    client = this.GetNextAvailableClient(ConnectionTimeoutManager.GetRemainingTimeout(_Timeout, TimeStamp, ioex));
                                }
                                else
                                {
                                    client.Dispose();
                                    client = null;
                                    throw new RetryExceededException(clientRetries, ioex);
                                }
                            }
                            else
                            {
                                client.Dispose();
                                client = null;
                                throw;
                            }
                        }
                        catch (TTransportException tex)
                        {
                            // if we have retried too many times, let's discontinue the execution
                            if (clientRetries < this.OriginalClients)
                            {
                                // keep trying till n-1, n = Number of Connections in the Pool
                                this._FaultyClients.Enqueue(client);
                                client = this.GetNextAvailableClient(ConnectionTimeoutManager.GetRemainingTimeout(_Timeout, TimeStamp, tex));
                            }
                            else
                            {
                                client.Dispose();
                                client = null;
                                throw new RetryExceededException(clientRetries, tex);
                            }
                        }
                        catch (SocketException sex)
                        {
                            if (clientRetries < this.OriginalClients)
                            {
                                // keep trying till n-1, n = Number of Connections in the Pool
                                this._FaultyClients.Enqueue(client);
                                client = this.GetNextAvailableClient(ConnectionTimeoutManager.GetRemainingTimeout(_Timeout, TimeStamp, sex));
                            }
                            else
                            {
                                client.Dispose();
                                client = null;
                                throw new RetryExceededException(clientRetries, sex);
                            }
                        }
                        catch
                        {
                            client.Dispose();
                            client = null;
                            throw;
                        }
                    }
                    catch (Exception e)
                    {
                        Type t = e.GetType();

                        //If the exception is not serializable or does not contain a serialization constructor, repackage it as a SerializableHBaseException and rethrow.
                        if ((object)e != null && (!t.IsSerializable || (object)t.GetConstructor(CONSTRUCTORATTRIBUTES, null,
                            new Type[] { typeof(SerializationInfo), typeof(StreamingContext) }, null) == null))
                        {
                            SerializableHBaseException serializableException = new SerializableHBaseException(e.Message, e);
                            serializableException.HelpLink = e.HelpLink;
                            serializableException.Source = e.Source;

                            throw serializableException;
                        }

                        throw;
                    }

                    // we are retrying
                    clientRetries++;
                } while ((object)client != null);
            }
            catch (Exception e)
            {
                Logger.Log.Error("An exception is being thrown from the client.", e);

                throw;
            }
        }

        public T Execute<T>(Func<IPooledHBaseClient, T> op)
        {
            try
            {
                DateTime TimeStamp = DateTime.Now;
                int clientRetries = 1;
                IPooledHBaseClient client = this.GetNextAvailableClient(ConnectionTimeoutManager.GetRemainingTimeout(_Timeout, TimeStamp));
                do
                {
                    try
                    {
                        try
                        {
                            T ret = op.Invoke(client);
                            client.Dispose();
                            client = null;
                            return ret;
                        }
                        catch (IOException ioex)
                        {
                            if (ioex.InnerException is SocketException
                                && (ioex.InnerException as SocketException).SocketErrorCode != SocketError.TimedOut)
                            {
                                // if we have retried too many times, let's discontinue the execution
                                if (clientRetries < this.OriginalClients)
                                {
                                    // keep trying till n-1, n = Number of Connections in the Pool
                                    this._FaultyClients.Enqueue(client);
                                    client = this.GetNextAvailableClient(ConnectionTimeoutManager.GetRemainingTimeout(_Timeout, TimeStamp, ioex));
                                }
                                else
                                {
                                    client.Dispose();
                                    client = null;
                                    throw new RetryExceededException(clientRetries, ioex);
                                }
                            }
                            else
                            {
                                client.Dispose();
                                client = null;
                                throw;
                            }
                        }
                        catch (TTransportException tex)
                        {
                            if (clientRetries < this.OriginalClients)
                            {
                                // keep trying till n-1, n = Number of Connections in the Pool
                                this._FaultyClients.Enqueue(client);
                                client = this.GetNextAvailableClient(ConnectionTimeoutManager.GetRemainingTimeout(_Timeout, TimeStamp, tex));
                            }
                            else
                            {
                                client.Dispose();
                                client = null;
                                throw new RetryExceededException(clientRetries, tex);
                            }
                        }
                        catch (SocketException sex)
                        {
                            if (clientRetries < this.OriginalClients)
                            {
                                // keep trying till n-1, n = Number of Connections in the Pool
                                this._FaultyClients.Enqueue(client);
                                client = this.GetNextAvailableClient(ConnectionTimeoutManager.GetRemainingTimeout(_Timeout, TimeStamp, sex));
                            }
                            else
                            {
                                client.Dispose();
                                client = null;
                                throw new RetryExceededException(clientRetries, sex);
                            }
                        }
                        catch
                        {
                            client.Dispose();
                            client = null;
                            throw;
                        }
                    }
                    catch (Exception e)
                    {
                        Type t = e.GetType();

                        //If the exception is not serializable or does not contain a serialization constructor, repackage it as a SerializableHBaseException and rethrow.
                        if ((object)e != null && (!t.IsSerializable || (object)t.GetConstructor(CONSTRUCTORATTRIBUTES, null,
                            new Type[] { typeof(SerializationInfo), typeof(StreamingContext) }, null) == null))
                        {
                            SerializableHBaseException serializableException = new SerializableHBaseException(e.Message, e);
                            serializableException.HelpLink = e.HelpLink;
                            serializableException.Source = e.Source;

                            throw serializableException;
                        }

                        throw;
                    }

                    // we are retrying
                    clientRetries++;
                } while (client != null);

                throw new InvalidOperationException("No Connection/Client available");
            }
            catch (Exception e)
            {
                Logger.Log.Error("An exception is being thrown from the HBase client.", e);

                throw;
            }
        }

        private bool FaultyClientsPollRunning = false;

        public void PollFaultyClients()
        {
            this.PollFaultyClients(-1);
        }

        protected void PollFaultyClients(Object Timeout)
        {
            DateTime TimeStamp = DateTime.Now;
            int iTimeout = (int)Timeout;

            try
            {
                if (iTimeout >= 0)
                {
                    if (Monitor.TryEnter(_FaultyClients, ConnectionTimeoutManager.GetRemainingTimeout(iTimeout, TimeStamp)))
                    {
                        try
                        {
                            if (FaultyClientsPollRunning)
                            {
                                return;
                            }

                            FaultyClientsPollRunning = true;
                        }
                        finally
                        {
                            Monitor.Exit(_FaultyClients);
                        }
                    }
                }
                else
                {
                    lock (_FaultyClients)
                    {
                        if (FaultyClientsPollRunning)
                        {
                            return;
                        }

                        FaultyClientsPollRunning = true;
                    }
                }

                List<IPooledHBaseClient> stillFailingClients = new List<IPooledHBaseClient>();

                try
                {
                    IPooledHBaseClient client;
                    while (_FaultyClients.TryDequeue(out client))
                    {
                        Logger.Log.Info("Attempting to revive faulty client for server: " + client.Host + ".");

                        try
                        {
                            client.Reset();

                            if (iTimeout >= 0)
                            {
                                client.Timeout = ConnectionTimeoutManager.GetRemainingTimeout(iTimeout, TimeStamp);
                            }
                            else
                            {
                                client.Timeout = _Timeout;
                            }

                            client.Execute(c => c.isTableEnabled(ClientEncoder.EncodeString(_CanaryTable)));
                            client.Dispose();
                            client = null;

                            if (iTimeout >= 0)
                            {
                                return;
                            }
                        }
                        catch (IOError e)
                        {
                            // connection is back alive...
                            client.Dispose();
                            client = null;

                            Logger.Log.Debug("Caught IOError. Connection appears to be back online.", e);

                            if (iTimeout >= 0)
                            {
                                return;
                            }
                        }
                        catch (TApplicationException e)
                        {
                            // connection is back alive...
                            client.Dispose();
                            client = null;

                            Logger.Log.Debug("Caught TApplicationException. Connection appears to be back online.", e);

                            if (iTimeout >= 0)
                            {
                                return;
                            }
                        }
                        catch (TTransportException e)
                        {
                            //connection is back alive
                            client.Dispose();
                            client = null;

                            Logger.Log.Debug("Caught TTransportException. Connection appears to be back online.", e);

                            if (iTimeout >= 0)
                            {
                                return;
                            }
                        }
                        catch (IOException ioex)
                        {
                            if (!(ioex.InnerException is SocketException))
                            {
                                // connection is back alive...
                                client.Dispose();
                                client = null;

                                Logger.Log.Debug("Caught IOException where InnerException is SocketException. Connection appears to be back online.", ioex);

                                if (iTimeout >= 0)
                                {
                                    return;
                                }
                            }
                            else
                            {
                                Logger.Log.Warn("Caught IOException where InnerException is not SocketException. Connection appears to be bad.", ioex);

                                if (iTimeout >= 0)
                                {
                                    //do not mask the exception if we are out of time
                                    ConnectionTimeoutManager.GetRemainingTimeout(iTimeout, TimeStamp, ioex);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.Log.Warn("Caught Exception. Connection appears to be bad.", ex);

                            if (iTimeout >= 0)
                            {
                                //do not mask the exception if we are out of time
                                ConnectionTimeoutManager.GetRemainingTimeout(iTimeout, TimeStamp, ex);
                            }
                        } // any other exception means the connection should still be kept as failing.
                        finally
                        {
                            if (client != null)
                            {
                                stillFailingClients.Add(client);
                            }
                        }
                    }
                }
                finally
                {
                    // requeue all clients still failing
                    foreach (var stillFailingClient in stillFailingClients)
                    {
                        this._FaultyClients.Enqueue(stillFailingClient);
                    }

                    //this needs to happen, so lock indefinently until we can set to false.
                    lock (this._FaultyClients)
                    {
                        FaultyClientsPollRunning = false;
                    }
                }
            }
            catch (Exception)
            {
                if (iTimeout >= 0)
                {
                    throw;
                }
            }
        }

        #region IDisposable Support
        private bool Disposing = false;
        private bool Disposed = false;

        protected virtual void Dispose(bool Disposing)
        {
            this.Disposing = true;
            if (!Disposed && Disposing)
            {
                IPooledHBaseClient Client;
                while (_FaultyClients.TryDequeue(out Client))
                {
                    Client.Dispose();
                }

                foreach (IHBaseConnection Conn in _AvailableConnections.ToArray())
                {
                    if ((object)Conn != null)
                    {
                        Conn.Dispose();
                    }
                }

                foreach (ClientSynchronizationEvent ClientSynchronization in _ClientSynchronization.ToArray())
                {
                    ClientSynchronization.ThrowExceptionOnSuspendedThread();
                }

                _ClientSynchronization = null;
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

    internal delegate void HBaseClientOperation(IPooledHBaseClient c);

    internal interface IHBaseClientPool
        : IDisposable
    {
        int AvailableClients { get; }
        int BlockedRequests { get; }
        int TotalClients { get; }
        void Execute(HBaseClientOperation op);
        T Execute<T>(Func<IPooledHBaseClient, T> op);
        void PollFaultyClients();
    }
}