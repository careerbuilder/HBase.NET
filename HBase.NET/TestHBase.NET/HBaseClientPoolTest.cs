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
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Rhino.Mocks;
using Thrift.Transport;
using Hbase;
using System.Threading;
using System.IO;
using System.Collections.Concurrent;
using Thrift;
using System.Net.Sockets;
using TestHBase.NET.TestDummies;

namespace TestHBase.NET
{
    [TestClass]
    public class HBaseClientPoolTest
    {
        private const int WAITMS = 30;

        [TestMethod]
        public void GuaranteesFairDeliveryOfClients()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnions(1);
            using (HBaseClientPool Pool = new HBaseClientPool(Clients))
            {
                // Arrange
                // fire a Thread cause the 1st thread takes longer to load the infrastructure
                new Thread(() => Console.WriteLine("Loading Thread Infrastructure...")).Start();
                Thread.Sleep(1500);

                // Act
                ConcurrentQueue<int> outputOrder = new ConcurrentQueue<int>();
                List<Thread> threads = new List<Thread>();
                for (int i = 0; i < 50; i++)
                {
                    var thisOrder = i;
                    var t = new Thread(() =>
                    {
                        Pool.Execute(cp =>
                        {
                            Console.WriteLine("Processing {0}", thisOrder);
                            Thread.Sleep(20);
                            outputOrder.Enqueue(thisOrder);
                        });
                    });
                    t.Start();
                    threads.Add(t);
                    Thread.Sleep(40);
                }

                foreach (var t in threads)
                    t.Join();

                // Assert
                int? prevValue = null;
                foreach (var value in outputOrder)
                {
                    if (prevValue.HasValue)
                        Assert.IsTrue(prevValue < value);
                    prevValue = value;
                }
            }
        }

        [TestMethod]
        public void CanHandleStressLoad()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnions();
            using (HBaseClientPool Pool = new HBaseClientPool(Clients))
            {
                // Arrange
                List<Thread> StressThreads = new List<Thread>();
                long operationCount = 0;
                long operationsExecuted = 0;

                // Act
                // Stress for 30 seconds
                DateTime stopTime = DateTime.Now.AddSeconds(30);
                for (int sleep = 1; sleep <= 20; sleep++)
                {
                    var thisCycleSleep = sleep;
                    var st = new Thread(() =>
                    {
                        while (DateTime.Now < stopTime)
                        {
                            Interlocked.Increment(ref operationCount);
                            Pool.Execute(cp =>
                            {
                                // pretend to do some work
                                Thread.Sleep(thisCycleSleep);
                                Interlocked.Increment(ref operationsExecuted);
                            });
                        }
                    });

                    st.Start();
                    StressThreads.Add(st);
                }

                // Assert

                // wait for all to finish
                foreach (var st in StressThreads)
                {
                    st.Join();
                }

                Assert.AreEqual(Clients.Count(), Pool.AvailableClients);
                Assert.AreEqual(operationCount, operationsExecuted);
            }

        }

        [TestMethod]
        public void CanHandleStressLoadWithErrors()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnionsWithApplicationErrors(10, 10, HBaseErrorContinuity.Always)
                .Union(GetMockClientTransportUnionsWithHBaseErrors(0, 10, HBaseErrorContinuity.Always))
                .Union(GetMockClientTransportUnionsWithNetworkErrors(0, 10, HBaseErrorContinuity.Always));

            using (HBaseClientPool Pool = new HBaseClientPool(Clients))
            {
                // Arrange
                List<Thread> StressThreads = new List<Thread>();
                long operationCount = 0;
                long operationsStarted = 0;
                long operationsFinished = 0;

                // Act
                // Stress for 30 seconds
                DateTime stopTime = DateTime.Now.AddSeconds(30);
                for (int sleep = 1; sleep <= 20; sleep++)
                {
                    var thisCycleSleep = sleep;
                    var st = new Thread(() =>
                    {
                        while (DateTime.Now < stopTime)
                        {
                            Interlocked.Increment(ref operationCount);
                            try
                            {
                                Pool.Execute(cp =>
                                {
                                    // pretend to do some work
                                    Thread.Sleep(thisCycleSleep);
                                    Interlocked.Increment(ref operationsStarted);
                                    cp.Execute(c => c.get(null, null, null, null));
                                    Interlocked.Increment(ref operationsFinished);
                                });
                            }
                            catch (Exception)
                            {
                                /* we expect errors */
                                Interlocked.Increment(ref operationsFinished);
                            }
                        }
                    });

                    st.Start();
                    StressThreads.Add(st);
                }

                // Assert

                // wait for all to finish
                foreach (var st in StressThreads)
                {
                    st.Join();
                }

                Assert.AreEqual(Clients.Count() - 10 /* 10 faulty network clients */, Pool.AvailableClients);
                Assert.IsTrue(operationCount < operationsStarted);
                Assert.AreEqual(operationCount, operationsFinished);
            }
        }

        [TestMethod]
        public void CanHandleStressLoadWithIntermitentErrors()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnionsWithApplicationErrors(2, 2, HBaseErrorContinuity.EveryTwo)
                .Union(GetMockClientTransportUnionsWithHBaseErrors(0, 2, HBaseErrorContinuity.EveryTwo))
                .Union(GetMockClientTransportUnionsWithNetworkErrors(0, 4, HBaseErrorContinuity.EveryTwo))
                .Union(GetMockClientTransportUnionsWithNetworkErrors(0, 4, HBaseErrorContinuity.Always));

            using (HBaseClientPool Pool = new HBaseClientPool(Clients))
            {
                // Arrange
                List<Thread> StressThreads = new List<Thread>();
                long operationCount = 0;
                long operationsStarted = 0;
                long operationsFinished = 0;

                // Act
                // Stress for 30 seconds
                DateTime stopTime = DateTime.Now.AddSeconds(30);
                for (int sleep = 1; sleep <= 25; sleep++)
                {
                    var thisCycleSleep = sleep;
                    var st = new Thread(() =>
                    {
                        while (DateTime.Now < stopTime)
                        {
                            Interlocked.Increment(ref operationCount);
                            try
                            {
                                Pool.Execute(cp =>
                                {
                                    // pretend to do some work
                                    Thread.Sleep(thisCycleSleep);
                                    Interlocked.Increment(ref operationsStarted);
                                    cp.Execute(c => c.get(null, null, null, null));
                                    Interlocked.Increment(ref operationsFinished);
                                });
                            }
                            catch (Exception) 
                            { 
                                /* we expect errors */
                                Interlocked.Increment(ref operationsFinished);
                            }
                        }
                    });

                    st.Start();
                    StressThreads.Add(st);
                }

                // Assert
                Thread.Sleep(2000);

                // We should have blocked requests at this point
                Assert.IsTrue(Pool.BlockedRequests > 0);

                // wait for all to finish
                foreach (var st in StressThreads)
                {
                    st.Join();
                }

                Assert.AreEqual(Clients.Count(), Pool.TotalClients);
                Assert.IsTrue(Pool.AvailableClients >= 2);
                Assert.IsTrue(Pool.AvailableClients <= 12);
                Assert.IsTrue(Pool.TotalClients - Pool.AvailableClients >= 2);
                Assert.IsTrue(Pool.TotalClients - Pool.AvailableClients <= 12);
                Assert.IsTrue(operationCount < operationsStarted);
                Assert.AreEqual(operationCount, operationsFinished);
            }
        }

        [TestMethod]
        public void DoesNotLeakConnectionsOnFaultyPolls()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnionsWithNetworkErrors(0, 4, HBaseErrorContinuity.Always)
                .Union(GetMockClientTransportUnionsWithNetworkErrors(0, 1, HBaseErrorContinuity.Twice));

            using (HBaseClientPool Pool = new HBaseClientPool(Clients))
            {
                List<Thread> threads = new List<Thread>();
                int operationsStarted = 0;
                int operationsFinished = 0;
                // need another thread to hold a connection
                // so that the pool request to timeout doesn't reach the Retry limit
                for (int i = 0; i < 2; i++)
                {
                    var t = new Thread(() =>
                    {
                        Pool.Execute(cp => cp.Execute(c =>
                        {
                            Thread.Sleep(12000);
                        }));
                    });
                    t.Start();
                    threads.Add(t);
                }

                Thread.Sleep(500); // allow threads to start

                Pool.Execute(cp => cp.Execute(c =>
                    {
                        Interlocked.Increment(ref operationsStarted);
                        c.get(null, null, null, null);
                        Interlocked.Increment(ref operationsFinished);
                    }));

                foreach (var t in threads)
                {
                    t.Join();
                }
                
                Assert.AreEqual(Clients.Count(), Pool.TotalClients);
                Assert.AreEqual(3, Pool.AvailableClients);
                Assert.AreEqual(2, Pool.TotalClients - Pool.AvailableClients);
                Assert.AreEqual(7, operationsStarted);
                Assert.AreEqual(1, operationsFinished);
            }
        }

        [TestMethod]
        public void GetNextAvailableClient_DoesNotSaturateConnectionPool()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnions(2);

            using (HBaseClientPool Pool = new HBaseClientPool(Clients))
            {
                bool closeFirst = false;
                new Thread(() =>
                    Pool.Execute(cp =>
                    {
                        while (!closeFirst)
                            Thread.Sleep(10);
                    })).Start();
                new Thread(() => Pool.Execute(cp =>
                    {
                        Thread.Sleep(2000);
                    })).Start();

                Thread th = new Thread(() => Pool.Execute(cp => { }));
                th.Start();

                Assert.IsFalse(th.Join(WAITMS));

                closeFirst = true;

                Assert.IsTrue(th.Join(WAITMS));
            }
        }

        [TestMethod]
        public void GetNextAvailableClient_AllowsIndividualConnectionsToContinueAfterConnectionPoolIsDisposed()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnions(2);

            using (HBaseClientPool Pool = new HBaseClientPool(Clients))
            {
                bool closeFirst = false;
                new Thread(() =>
                    Pool.Execute(cp =>
                    {
                        while (!closeFirst)
                            Thread.Sleep(10);
                    })).Start();
                new Thread(() => Pool.Execute(cp =>
                {
                    Thread.Sleep(10000);
                })).Start();

                Thread.Sleep(200); // allow for threads to start

                Thread th = new Thread(() => Pool.Execute(cp => { }));
                th.Start();

                Assert.IsFalse(th.Join(WAITMS));

                closeFirst = true;
                Thread.Sleep(50); // allow for thread to end

                Pool.Dispose();

                Assert.IsTrue(th.Join(WAITMS));
            }
        }

        [TestMethod, Timeout(2000)]
        public void GetNextAvailableClient_ThrowsExceptionForWaitingThreadsOnDisposalOfConnectionPool()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnions(2);

            using (HBaseClientPool Pool = new HBaseClientPool(Clients))
            {
                bool closeFirst = false;
                new Thread(() =>
                    Pool.Execute(cp =>
                    {
                        while (!closeFirst)
                            Thread.Sleep(10);
                    })).Start();
                new Thread(() => Pool.Execute(cp =>
                {
                    Thread.Sleep(1000);
                })).Start();

                bool ExceptionCaught = false;

                Thread.Sleep(200); // allow for threads to start

                Thread th = new Thread(() => CatchExpectedExceptionForNextAvailableClient(Pool, out ExceptionCaught));
                th.Start();

                Assert.IsFalse(th.Join(WAITMS));

                Pool.Dispose();

                th.Join();

                Assert.IsTrue(ExceptionCaught);
            }
        }

        [TestMethod]
        public void ForwardsApplicationExceptions()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnionsWithApplicationErrors(0, 5, HBaseErrorContinuity.Always);

            using (HBaseClientPool Pool = new HBaseClientPool(Clients))
            {
                List<Thread> operationThreads = new List<Thread>();
                int operationsExecuted = 0;
                int exceptionsForwarded = 0;

                for (int ti = 0; ti < Clients.Count(); ti++)
                {
                    var t = new Thread(() =>
                    {
                        try
                        {
                            Interlocked.Increment(ref operationsExecuted);
                            Pool.Execute(cp => cp.Execute(c => c.get(null, null, null, null)));
                        }
                        catch (ApplicationException) { Interlocked.Increment(ref exceptionsForwarded); }
                        catch (Exception ex) { Console.WriteLine(ex.ToString()); }
                    });

                    t.Start();
                    operationThreads.Add(t);
                }

                // wait for all threads
                foreach (var t in operationThreads)
                {
                    t.Join();
                }


                // Assert
                Assert.AreEqual(operationsExecuted, exceptionsForwarded);
                Assert.AreEqual(Clients.Count(), Pool.AvailableClients);
            }
        }

        [TestMethod]
        public void ForwardsHBaseExceptions()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnionsWithHBaseErrors(0, 5, HBaseErrorContinuity.Always);

            using (HBaseClientPool Pool = new HBaseClientPool(Clients))
            {
                List<Thread> operationThreads = new List<Thread>();
                int operationsExecuted = 0;
                int exceptionsForwarded = 0;

                for (int ti = 0; ti < Clients.Count(); ti++)
                {
                    var t = new Thread(() =>
                    {
                        try
                        {
                            Interlocked.Increment(ref operationsExecuted);
                            Pool.Execute(cp => cp.Execute(c => c.get(null, null, null, null)));
                        }
                        catch (TApplicationException)
                        {
                            Interlocked.Increment(ref exceptionsForwarded);
                        }
                        catch (IOError)
                        {
                            Interlocked.Increment(ref exceptionsForwarded);
                        }
                        catch (IOException ioex)
                        {
                            if (ioex.InnerException is SocketException
                                && (ioex.InnerException as SocketException).SocketErrorCode == SocketError.TimedOut)
                                Interlocked.Increment(ref exceptionsForwarded);
                        }
                        catch (SerializableHBaseException e)
                        {
                            if ((object)e.InnerException != null && (e.InnerException is TApplicationException || e.InnerException is IOError || (e.InnerException is IOException &&
                                e.InnerException.InnerException is SocketException && (e.InnerException.InnerException as SocketException).SocketErrorCode == SocketError.TimedOut)))
                            {
                                Interlocked.Increment(ref exceptionsForwarded);
                            }
                            else
                            {
                                Console.WriteLine(e.ToString());
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.ToString());
                        }
                    });

                    t.Start();
                    operationThreads.Add(t);
                }

                // wait for all threads
                foreach (var t in operationThreads)
                {
                    t.Join();
                }


                // Assert
                Assert.AreEqual(operationsExecuted, exceptionsForwarded);
                Assert.AreEqual(Clients.Count(), Pool.AvailableClients);
            }
        }

        [TestMethod]
        public void SetsAsideFaultyConnections()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnionsWithNetworkErrors(5, 5, HBaseErrorContinuity.Always);

            using (HBaseClientPool Pool = new HBaseClientPool(Clients))
            {
                List<Thread> operationThreads = new List<Thread>();
                int operationsExecuted = 0;
                int exceptionsForwarded = 0;

                for (int ti = 0; ti < Clients.Count(); ti++)
                {
                    var t = new Thread(() =>
                    {
                        Thread.Sleep(1000);
                        Interlocked.Increment(ref operationsExecuted);
                        try
                        {
                            Pool.Execute(cp => cp.Execute(c => c.get(null, null, null, null)));
                        }
                        catch (Exception) { Interlocked.Increment(ref exceptionsForwarded); }
                    });

                    t.Start();
                    operationThreads.Add(t);
                }

                // wait for all threads
                foreach (var t in operationThreads)
                {
                    t.Join();
                }


                // Assert
                Assert.AreEqual(Clients.Count(), operationsExecuted);
                Assert.AreEqual(0, exceptionsForwarded);
                Assert.AreEqual(5, Pool.AvailableClients);
            }
        }

        [TestMethod]
        public void AutomaticallyRetriesNetworkErrors()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnionsWithNetworkErrors(0, 1, HBaseErrorContinuity.Once);

            using (HBaseClientPool Pool = new HBaseClientPool(Clients))
            {
                List<Thread> operationThreads = new List<Thread>();
                int operationsExecuted = 0;
                int exceptionsForwarded = 0;

                for (int ti = 0; ti < Clients.Count(); ti++)
                {
                    var t = new Thread(() =>
                    {
                        try
                        {
                            Pool.Execute(cp => cp.Execute(c =>
                            {
                                Interlocked.Increment(ref operationsExecuted);
                                c.get(null, null, null, null);
                            }));
                        }
                        catch (Exception) { Interlocked.Increment(ref exceptionsForwarded); }
                    });

                    t.Start();
                    operationThreads.Add(t);
                }

                Thread.Sleep(4500); // wait for thread to effectively start

                Assert.AreEqual(2, operationsExecuted); // 1 on first try, 2 on automatic retry due to network error
                Assert.AreEqual(1, Pool.AvailableClients); // Should be done by now

                // wait for all threads
                foreach (var t in operationThreads)
                {
                    t.Join();
                }

                // Assert
                Assert.AreEqual(2, operationsExecuted); // 1 on first try, 2 on automatic retry due to network error
                Assert.AreEqual(0, exceptionsForwarded);
                Assert.AreEqual(1, Pool.AvailableClients);
            }
        }

        [TestMethod]
        public void RecoversFaultyConnections()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnionsWithNetworkErrors(0, 3, HBaseErrorContinuity.Twice);

            using (HBaseClientPool Pool = new HBaseClientPool(Clients))
            {
                List<Thread> threads = new List<Thread>();
                int operationsExecuted = 0;
                int exceptionsForwarded = 0;

                // need another thread to hold a connection
                // so that the pool request to timeout doesn't reach the Retry limit
                for (int i = 0; i < 2; i++)
                {
                    var t = new Thread(() =>
                    {
                        Pool.Execute(cp => cp.Execute(c =>
                        {
                            Thread.Sleep(8000);
                        }));
                    });
                    t.Start();
                    threads.Add(t);
                }

                var t1 = new Thread(() =>
                {
                    try
                    {
                        Pool.Execute(cp => cp.Execute(c =>
                        {
                            Interlocked.Increment(ref operationsExecuted);
                            c.get(null, null, null, null);
                        }));
                    }
                    catch (Exception ex) { Console.WriteLine(ex.ToString()); Interlocked.Increment(ref exceptionsForwarded); }
                });

                t1.Start();
                threads.Add(t1);

                // wait for all threads
                foreach (var th in threads)
                {
                    th.Join();
                }

                // Assert
                Assert.AreEqual(3, operationsExecuted); // 1 on first try, 2 on automatic retry due to network error, 3 after client recovery
                Assert.AreEqual(0, exceptionsForwarded);
                Assert.AreEqual(3, Pool.AvailableClients);
            }
        }

        [TestMethod, Timeout(14000)]
        public void TimesOutWhenAllClientsDown()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnionsWithNetworkErrors(0, 3, HBaseErrorContinuity.Always);

            using (HBaseClientPool Pool = new HBaseClientPool(Clients, 11500))
            {
                int operationsExecuted = 0;
                bool gotException = false;

                // need another thread to hold a connection
                // so that the pool request to timeout doesn't reach the Retry limit
                var t = new Thread(() =>
                {
                    Pool.Execute(cp => cp.Execute(c =>
                        {
                            Thread.Sleep(12000);
                        }));
                });
                t.Start();

                try
                {
                    Pool.Execute(cp => cp.Execute(c =>
                    {
                        operationsExecuted++;
                        c.get(null, null, null, null);
                    }));
                }
                catch (TimeoutException) { gotException = true; }

                t.Join();

                Assert.IsTrue(gotException);
                Assert.AreEqual(1, Pool.AvailableClients);
                Assert.AreEqual(3, Pool.TotalClients);
                Assert.AreEqual(4, operationsExecuted);
            }
        }

        [TestMethod]
        public void WontRetryMoreThanNumberOfClients()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnionsWithNetworkErrors(0, 4, HBaseErrorContinuity.Always)
                .Union(GetMockClientTransportUnionsWithNetworkErrors(0, 1, HBaseErrorContinuity.Twice));

            using (HBaseClientPool Pool = new HBaseClientPool(Clients))
            {
                int operationsExecuted = 0;
                bool gotException = false;

                try
                {
                    Pool.Execute(cp => cp.Execute(c =>
                    {
                        operationsExecuted++;
                        c.get(null, null, null, null);
                    }));
                }
                catch (RetryExceededException) { gotException = true; }

                Assert.IsTrue(gotException);
                Assert.AreEqual(10, operationsExecuted);
            }
        }

        [TestMethod, ExpectedException(typeof (TimeoutException))]
        public void TimesOutAppropriatelyInHBaseClientPool()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnionsWithNetworkErrors(0, 5);

            using (HBaseClientPool Pool = new HBaseClientPool(Clients, 250))
            {
                Pool.Execute(cp => 
                {
                    Thread.Sleep(2000);

                    cp.Execute(c =>
                    {
                        c.get(null, null, null, null);
                    });
                });
            }
        }

        [TestMethod]
        public void ExecutesOperationsWhenNotExceedingTimeoutInHBaseClientPool()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnionsWithNetworkErrors(0, 5);

            int OperationsExecuted = 0;

            using (HBaseClientPool Pool = new HBaseClientPool(Clients, 2500))
            {
                Pool.Execute(cp =>
                {
                    Thread.Sleep(500);

                    cp.Execute(c =>
                    {
                        OperationsExecuted++;
                        c.get(null, null, null, null);
                    });
                });
            }

            Assert.AreEqual(2, OperationsExecuted);
        }

        [TestMethod, ExpectedException(typeof(TimeoutException))]
        public void TimesOutAppropriatelyInPooledHBaseClient()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnionsWithNetworkErrors(0, 5);

            using (HBaseClientPool Pool = new HBaseClientPool(Clients, 250))
            {
                Pool.Execute(cp =>
                {
                    cp.Execute(c =>
                    {
                        Thread.Sleep(500);
                        c.get(null, null, null, null);
                    });
                });
            }
        }

        [TestMethod]
        public void ExecutesOperationsWhenNotExceedingTimeoutInPooledHBaseClient()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnionsWithNetworkErrors(0, 5);

            int OperationsExecuted = 0;

            using (HBaseClientPool Pool = new HBaseClientPool(Clients, 2500))
            {
                Pool.Execute(cp =>
                {
                    cp.Execute(c =>
                    {
                        Thread.Sleep(500);
                        OperationsExecuted++;
                        c.get(null, null, null, null);
                    });
                });
            }

            Assert.AreEqual(2, OperationsExecuted);
        }

        [TestMethod, ExpectedException(typeof(SerializableHBaseException))]
        public void ThrowsSerializableHBaseExceptionIfThrownExceptionIsNotSerializable()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnionsWithNetworkErrors(0, 5);
            //IHBaseConnection Client = MockRepository.GenerateStub<IHBaseConnection>();

            using (HBaseClientPool Pool = new HBaseClientPool(Clients, 2500))
            {
                Pool.Execute(cp =>
                {
                    cp.Execute(c =>
                    {
                        throw new IOError();
                    });
                });
            }
        }

        [TestMethod, ExpectedException(typeof(Exception))]
        public void ThrowsUnderlyingExceptionIfThrownExceptionIsSerializable()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnionsWithNetworkErrors(0, 5);
            //IHBaseConnection Client = MockRepository.GenerateStub<IHBaseConnection>();

            using (HBaseClientPool Pool = new HBaseClientPool(Clients, 2500))
            {
                Pool.Execute(cp =>
                {
                    cp.Execute(c =>
                    {
                        throw new Exception();
                    });
                });
            }
        }

        [TestMethod, ExpectedException(typeof(SerializableHBaseException))]
        public void ThrowsSerializableHBaseExceptionIfThrownExceptionIsNotSerializableForGenericMethod()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnionsWithNetworkErrors(0, 5);
            //IHBaseConnection Client = MockRepository.GenerateStub<IHBaseConnection>();

            using (HBaseClientPool Pool = new HBaseClientPool(Clients, 2500))
            {
                Pool.Execute<object>(cp =>
                {
                    cp.Execute(c =>
                    {
                        throw new IOError();
                    });

                    return null;
                });
            }
        }

        [TestMethod, ExpectedException(typeof(Exception))]
        public void ThrowsUnderlyingExceptionIfThrownExceptionIsSerializableForGenericMethod()
        {
            IEnumerable<IHBaseConnection> Clients = GetMockClientTransportUnionsWithNetworkErrors(0, 5);
            //IHBaseConnection Client = MockRepository.GenerateStub<IHBaseConnection>();

            using (HBaseClientPool Pool = new HBaseClientPool(Clients, 2500))
            {
                Pool.Execute<object>(cp =>
                {
                    cp.Execute(c =>
                    {
                        throw new Exception();
                    });

                    return null;
                });
            }
        }

        #region Static Internals

        private static IEnumerable<IHBaseConnection> GetMockClientTransportUnions(int unions = 10)
        {
            TBufferedTransport MockTransport = MockRepository.GenerateStub<TBufferedTransport>(MockRepository.GenerateStub<TStreamTransport>());
            MockTransport.Stub(s => s.IsOpen).Return(true);

            IList<IHBaseConnection> Clients = new List<IHBaseConnection>();

            for (int i = 0; i < unions; ++i)
            {
                Clients.Add(new HBaseConnection(MockRepository.GenerateStub<Hbase.Hbase.Iface>(), MockTransport));
            }

            return Clients;
        }

        private static IEnumerable<IHBaseConnection> GetMockClientTransportUnionsWithApplicationErrors(int unions = 10, int applicationErrors = 5, HBaseErrorContinuity errorContinuity = HBaseErrorContinuity.Once)
        {
            IList<IHBaseConnection> Clients = new List<IHBaseConnection>();

            for (int i = 0; i < unions; ++i)
            {
                Clients.Add(new TestHBaseConnection());
            }

            for (int i = 0; i < applicationErrors; ++i)
            {
                Clients.Add(new TestHBaseConnection(errorContinuity, HBaseErrorType.Application));
            }

            return Clients;
        }

        private static IEnumerable<IHBaseConnection> GetMockClientTransportUnionsWithNetworkErrors(int unions = 10, int networkErrors = 5, HBaseErrorContinuity errorContinuity = HBaseErrorContinuity.Once)
        {
            IList<IHBaseConnection> Clients = new List<IHBaseConnection>();

            for (int i = 0; i < unions; ++i)
            {
                Clients.Add(new TestHBaseConnection());
            }

            for (int i = 0; i < networkErrors; ++i)
            {
                Clients.Add(new TestHBaseConnection(errorContinuity, HBaseErrorType.Network));
            }

            return Clients;
        }

        private static IEnumerable<IHBaseConnection> GetMockClientTransportUnionsWithHBaseErrors(int unions = 10, int hbaseErrors = 5, HBaseErrorContinuity errorContinuity = HBaseErrorContinuity.Once)
        {
            IList<IHBaseConnection> Clients = new List<IHBaseConnection>();

            for (int i = 0; i < unions; ++i)
            {
                Clients.Add(new TestHBaseConnection());
            }

            for (int i = 0; i < hbaseErrors; ++i)
            {
                Clients.Add(new TestHBaseConnection(errorContinuity, HBaseErrorType.HBase));
            }

            return Clients;
        }

        private static void CatchExpectedExceptionForNextAvailableClient(HBaseClientPool Pool, out bool ExceptionCaught)
        {
            ExceptionCaught = false;

            try
            {
                Pool.Execute(cp => { });
            }
            catch (Exception)
            {
                ExceptionCaught = true;
            }
        }
        #endregion
    }
}
