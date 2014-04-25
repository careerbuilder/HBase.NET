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
using Hbase;
using Rhino.Mocks;
using System.Collections;
using System.Reflection;
using System.IO;
using Hbase.StaticInternals;
using TestHBase.NET;
using TestHBase.NET.TestDummies;

namespace TestHBaseClient.NET
{
    [TestClass]
    public class HBaseClientTest
    {
        private const string TABLE = "SampleTable";
        private const string COLUMNFAMILY = "SampleColumnFamily";
        private const string COLUMN = "SampleColumn";
        private const string SERVER = "SampleHBaseServer.example.com";
        private const int PORT = 9090;
        private const string FOOCOLUMNFAMILY = "FooCF";
        private const string BARCOLUMNFAMILY = "BarCF";
        private const string KEY = "Key";
        private const string KEY2 = "Key2";
        private const string KEY3 = "Key3";
        private const int INTFIELD = 5;
        private const int INTPROPERTY = 7;
        private const string STRINGFIELD = "foo";
        private const string STRINGPROPERTY = "bar";
        private const long ATOMICVALUE = 33L;
        private const TimeInterval INTERVAL = TimeInterval.Hourly;

        private static DateTime TimeStamp
        {
            get
            {
                return new DateTime(1987, 7, 24, 4, 10, 5);
            }
        }

        private static DateTime StartTime
        {
            get
            {
                return new DateTime(1987, 7, 24, 6, 10, 5);
            }
        }

        private static DateTime StopTime
        {
            get
            {
                return new DateTime(1987, 7, 24, 7, 10, 5);
            }
        }

        [TestMethod]
        public void CreateTable_CallsClientWithAppropriateColumnDescriptors()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Client.CreateTable(TABLE, new string[] { FOOCOLUMNFAMILY, BARCOLUMNFAMILY }, CompressionType.Snappy);

                IList<object[]> Args = Face.GetArgumentsForCallsMadeOn(s => s.createTable(Arg<byte[]>.Is.Anything, Arg<List<ColumnDescriptor>>.Is.Anything));

                AssertEquivalentByteArray(Args.FirstOrDefault().First(), TABLE);

                IList<ColumnDescriptor> ColumnDescriptors = (IList<ColumnDescriptor>)Args.FirstOrDefault()[1];

                AssertEquivalentByteArray(ColumnDescriptors.First().Name, FOOCOLUMNFAMILY);
                Assert.AreEqual("SNAPPY", ColumnDescriptors.First().Compression);
                AssertEquivalentByteArray(ColumnDescriptors[1].Name, BARCOLUMNFAMILY);
                Assert.AreEqual("SNAPPY", ColumnDescriptors[1].Compression);
            }
        }

        [TestMethod]
        public void DeleteTable_MakesAppropriateCallsToClient()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Client.DeleteTable(TABLE);

                IList<object[]> DisableArgs = Face.GetArgumentsForCallsMadeOn(s => s.disableTable(Arg<byte[]>.Is.Anything));
                IList<object[]> DeleteArgs = Face.GetArgumentsForCallsMadeOn(s => s.deleteTable(Arg<byte[]>.Is.Anything));

                AssertEquivalentByteArray(DisableArgs.FirstOrDefault().First(), TABLE);
                AssertEquivalentByteArray(DeleteArgs.FirstOrDefault().First(), TABLE);
            }
        }

        [TestMethod]
        public void GetTableNames_ReturnsMockedResults()
        {
            const string FOOTABLE = "Foo";
            const string BARTABLE = "Bar";

            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                List<byte[]> TableBytes = new List<byte[]>();
                TableBytes.Add(ClientEncoder.EncodeString(FOOTABLE));
                TableBytes.Add(ClientEncoder.EncodeString(BARTABLE));

                Face.Stub(s => s.getTableNames()).Return(TableBytes);

                ICollection<string> TableNames = Client.GetTableNames();

                CollectionAssert.AreEquivalent(new object[] { FOOTABLE, BARTABLE }, (ICollection)TableNames);
            }
        }

        [TestMethod]
        public void PutOne_MutatesRowForExampleObject()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Client.PutOne(TABLE, KEY, COLUMNFAMILY, GetFilledExampleObject());

                IList<object[]> Args = Face.GetArgumentsForCallsMadeOn(s => s.mutateRow(Arg<byte[]>.Is.Anything, Arg<byte[]>.Is.Anything,
                    Arg<List<Mutation>>.Is.Anything, Arg<Dictionary<byte[], byte[]>>.Is.Anything));

                AssertEquivalentByteArray(Args.First().First(), TABLE);
                AssertEquivalentByteArray(Args.First()[1], KEY);

                IList<Mutation> Mutations = (IList<Mutation>)Args.First()[2];

                AssertExpectedMutationsForExampleObject(Mutations, false);
            }
        }

        [TestMethod]
        public void PutOne_MutatesRowForExampleNullableObject()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Client.PutOne(TABLE, KEY, COLUMNFAMILY, GetFilledExampleNullableObject());

                IList<object[]> Args = Face.GetArgumentsForCallsMadeOn(s => s.mutateRow(Arg<byte[]>.Is.Anything, Arg<byte[]>.Is.Anything,
                    Arg<List<Mutation>>.Is.Anything, Arg<Dictionary<byte[], byte[]>>.Is.Anything));

                AssertEquivalentByteArray(Args.First().First(), TABLE);
                AssertEquivalentByteArray(Args.First()[1], KEY);

                IList<Mutation> Mutations = (IList<Mutation>)Args.First()[2];

                AssertExpectedMutationsForExampleNullableObject(Mutations, false);
            }
        }

        [TestMethod]
        public void PutOne_MutatesRowForExampleInterface()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Client.PutOne(TABLE, KEY, COLUMNFAMILY, (IExampleObject)GetFilledExampleObject());

                IList<object[]> Args = Face.GetArgumentsForCallsMadeOn(s => s.mutateRow(Arg<byte[]>.Is.Anything, Arg<byte[]>.Is.Anything,
                    Arg<List<Mutation>>.Is.Anything, Arg<Dictionary<byte[], byte[]>>.Is.Anything));

                AssertEquivalentByteArray(Args.First().First(), TABLE);
                AssertEquivalentByteArray(Args.First()[1], KEY);

                IList<Mutation> Mutations = (IList<Mutation>)Args.First()[2];

                AssertExpectedMutationsForIExampleObject(Mutations, false);
            }
        }

        [TestMethod]
        public void PutBulk_MutatesRows()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                IList<IHBasePut> Bulk = new List<IHBasePut>();
                Bulk.Add(new HBasePut<ExampleObject>(KEY, COLUMNFAMILY, GetFilledExampleObject()));
                Bulk.Add(new HBasePut<ExampleObject>(KEY2, COLUMNFAMILY, GetFilledExampleObject()));

                Client.PutBulk(TABLE, Bulk);

                IList<object[]> Args = Face.GetArgumentsForCallsMadeOn(s => s.mutateRows(Arg<byte[]>.Is.Anything, Arg<List<BatchMutation>>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything));

                AssertEquivalentByteArray(Args.First().First(), TABLE);

                IList<BatchMutation> BatchMutations = (IList<BatchMutation>)Args.First()[1];

                Assert.AreEqual(2, BatchMutations.Count());

                foreach (BatchMutation Batch in BatchMutations)
                {
                    if (!(Enumerable.SequenceEqual(Batch.Row, ClientEncoder.EncodeString(KEY)) || Enumerable.SequenceEqual(Batch.Row,
                        ClientEncoder.EncodeString(KEY2))))
                    {
                        Assert.Fail();
                    }

                    AssertExpectedMutationsForExampleObject(Batch.Mutations, false);
                }
            }
        }

        [TestMethod]
        public void DeleteOne_MutatesRowWithDeletionSet()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Client.DeleteOne<ExampleObject>(TABLE, KEY, COLUMNFAMILY);

                IList<object[]> Args = Face.GetArgumentsForCallsMadeOn(s => s.mutateRow(Arg<byte[]>.Is.Anything, Arg<byte[]>.Is.Anything,
                    Arg<List<Mutation>>.Is.Anything, Arg<Dictionary<byte[], byte[]>>.Is.Anything));

                AssertEquivalentByteArray(Args.First().First(), TABLE);
                AssertEquivalentByteArray(Args.First()[1], KEY);

                IList<Mutation> Mutations = (IList<Mutation>)Args.First()[2];

                AssertExpectedMutationsForIExampleObject(Mutations, true);
            }
        }

        [TestMethod]
        public void DeleteBulk_MutatesRowsWithDeletionSet()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                IList<IHBaseDeletion> Bulk = new List<IHBaseDeletion>();
                Bulk.Add(new HBaseDeletion<ExampleObject>(KEY, COLUMNFAMILY));
                Bulk.Add(new HBaseDeletion<ExampleObject>(KEY2, COLUMNFAMILY));

                Client.DeleteBulk(TABLE, Bulk);

                IList<object[]> Args = Face.GetArgumentsForCallsMadeOn(s => s.mutateRows(Arg<byte[]>.Is.Anything, Arg<List<BatchMutation>>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything));

                AssertEquivalentByteArray(Args.First().First(), TABLE);

                IList<BatchMutation> BatchMutations = (IList<BatchMutation>)Args.First()[1];

                Assert.AreEqual(2, BatchMutations.Count());

                foreach (BatchMutation Batch in BatchMutations)
                {
                    if (!(Enumerable.SequenceEqual(Batch.Row, ClientEncoder.EncodeString(KEY)) || Enumerable.SequenceEqual(Batch.Row,
                        ClientEncoder.EncodeString(KEY2))))
                    {
                        Assert.Fail();
                    }

                    AssertExpectedMutationsForExampleObject(Batch.Mutations, true);
                }
            }
        }

        [TestMethod]
        public void GetOne_ReturnsNothingForEmptyResultsSet()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Face.Stub(s => s.getRowWithColumns(Arg<byte[]>.Is.Anything, Arg<byte[]>.Is.Anything, Arg<List<byte[]>>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything)).Return(new List<TRowResult>());

                ExampleObject Example = Client.GetOne<ExampleObject>(TABLE, KEY, COLUMNFAMILY);

                Assert.IsNull(Example);
            }
        }

        [TestMethod]
        public void GetOne_ReturnsConcreteObject()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Dictionary<byte[], TCell> ReturnDictionary = GetReturnDictionaryForExampleObject();

                List<TRowResult> ReturnResults = new List<TRowResult>();
                ReturnResults.Add(new TRowResult()
                {
                    Columns = ReturnDictionary,
                    Row = ClientEncoder.EncodeString(KEY)
                });

                Face.Stub(s => s.getRowWithColumns(Arg<byte[]>.Is.Anything, Arg<byte[]>.Is.Anything, Arg<List<byte[]>>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything)).Return(ReturnResults);

                ExampleObject Example = Client.GetOne<ExampleObject>(TABLE, KEY, COLUMNFAMILY);

                AssertEqualsFilledExampleObject(Example);
            }
        }

        [TestMethod]
        public void GetOne_ReturnsDynamicProxy()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Dictionary<byte[], TCell> ReturnDictionary = GetReturnDictionaryForIExampleObject();

                List<TRowResult> ReturnResults = new List<TRowResult>();
                ReturnResults.Add(new TRowResult()
                {
                    Columns = ReturnDictionary,
                    Row = ClientEncoder.EncodeString(KEY)
                });

                Face.Stub(s => s.getRowWithColumns(Arg<byte[]>.Is.Anything, Arg<byte[]>.Is.Anything, Arg<List<byte[]>>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything)).Return(ReturnResults);

                IExampleObject Example = Client.GetOne<IExampleObject>(TABLE, KEY, COLUMNFAMILY);

                Assert.AreEqual(INTPROPERTY, Example.IntProperty);
                Assert.AreEqual(STRINGPROPERTY, Example.StringProperty);
                Assert.AreEqual(DateTime.MaxValue.ToString(ClientEncoder.DATEFORMAT), Example.DateTimeProperty.ToString(ClientEncoder.DATEFORMAT));
            }
        }

        [TestMethod]
        public void GetOneWithTimeStamp_ReturnsDynamicProxy()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Dictionary<byte[], TCell> ReturnDictionary = GetTimeSeriesReturnDictionaryForIExampleObject(TimeStamp);

                List<TRowResult> ReturnResults = new List<TRowResult>();
                ReturnResults.Add(new TRowResult()
                {
                    Columns = ReturnDictionary,
                    Row = ClientEncoder.EncodeString(KEY)
                });

                Face.Stub(s => s.getRowWithColumns(Arg<byte[]>.Is.Anything, Arg<byte[]>.Is.Anything, Arg<List<byte[]>>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything)).Return(ReturnResults);

                IExampleObject Example = Client.GetOne<IExampleObject>(TABLE, KEY, TimeStamp, INTERVAL);

                Assert.AreEqual(INTPROPERTY, Example.IntProperty);
                Assert.AreEqual(STRINGPROPERTY, Example.StringProperty);
                Assert.AreEqual(DateTime.MaxValue.ToString(ClientEncoder.DATEFORMAT), Example.DateTimeProperty.ToString(ClientEncoder.DATEFORMAT));
            }
        }

        [TestMethod]
        public void GetRange_ReturnsDynamicProxy()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Dictionary<byte[], TCell> ReturnDictionary = GetTimeSeriesRangeReturnDictionaryForIExampleObject();

                List<TRowResult> ReturnResults = new List<TRowResult>();
                ReturnResults.Add(new TRowResult()
                {
                    Columns = ReturnDictionary,
                    Row = ClientEncoder.EncodeString(KEY)
                });

                Face.Stub(s => s.getRowWithColumns(Arg<byte[]>.Is.Anything, Arg<byte[]>.Is.Anything, Arg<List<byte[]>>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything)).Return(ReturnResults);

                IDictionary<DateTime, IExampleObject> Examples = Client.GetRange<IExampleObject>(TABLE, KEY, StartTime, StopTime, INTERVAL);

                Assert.AreEqual(INTPROPERTY, Examples.Values.First().IntProperty);
                Assert.AreEqual(STRINGPROPERTY, Examples.Values.First().StringProperty);
                Assert.AreEqual(DateTime.MaxValue.ToString(ClientEncoder.DATEFORMAT),
                    Examples.Values.First().DateTimeProperty.ToString(ClientEncoder.DATEFORMAT));

                CollectionAssert.AreEquivalent(new DateTime[] { StartTime.Date.AddHours(StartTime.Hour), StopTime.Date.AddHours(StopTime.Hour) },
                    (ICollection)Examples.Keys);
            }
        }

        [TestMethod]
        public void GetRange_ReturnsDynamicProxies()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                List<TRowResult> ReturnResults = new List<TRowResult>();
                ReturnResults.Add(new TRowResult()
                {
                    Columns = GetTimeSeriesRangeReturnDictionaryForIExampleObject(),
                    Row = ClientEncoder.EncodeString(KEY)
                });
                ReturnResults.Add(new TRowResult()
                {
                    Columns = GetTimeSeriesRangeReturnDictionaryForIExampleObject(),
                    Row = ClientEncoder.EncodeString(KEY2)
                });
                ReturnResults.Add(new TRowResult()
                {
                    Columns = GetTimeSeriesRangeReturnDictionaryForIExampleObject(),
                    Row = ClientEncoder.EncodeString(KEY3)
                });

                Face.Stub(s => s.getRowsWithColumns(Arg<byte[]>.Is.Anything, Arg<List<byte[]>>.Is.Anything, Arg<List<byte[]>>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything)).Return(ReturnResults);

                IDictionary<String, IDictionary<DateTime, IExampleObject>> Examples = Client.GetRange<IExampleObject>(TABLE, new List<string> { KEY, KEY2, KEY3 }, StartTime, StopTime, INTERVAL);

                Assert.AreEqual(3, Examples.Count);
                Assert.AreEqual(INTPROPERTY, Examples[KEY].Values.First().IntProperty);
                Assert.AreEqual(STRINGPROPERTY, Examples[KEY].Values.First().StringProperty);
                Assert.AreEqual(DateTime.MaxValue.ToString(ClientEncoder.DATEFORMAT),
                    Examples[KEY].Values.First().DateTimeProperty.ToString(ClientEncoder.DATEFORMAT));

                CollectionAssert.AreEquivalent(new String[] { KEY, KEY2, KEY3 }, (ICollection)Examples.Keys);
                CollectionAssert.AreEquivalent(new DateTime[] { StartTime.Date.AddHours(StartTime.Hour), StopTime.Date.AddHours(StopTime.Hour) },
                    (ICollection)Examples[KEY].Keys);
            }
        }

        [TestMethod]
        public void GetBulk_ReturnsExampleObjects()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Face.Stub(s => s.getRowsWithColumns(Arg<byte[]>.Is.Anything, Arg<List<byte[]>>.Is.Anything, Arg<List<byte[]>>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything)).Return(
                    GetReturnResultsForExampleObject());

                IDictionary<string, ExampleObject> ExamplesDictionary = Client.GetBulk<ExampleObject>(TABLE, new string[] { KEY, KEY2, KEY3 }, COLUMNFAMILY);

                AssertEqualsFilledExampleObjectIDictionary(ExamplesDictionary);
            }
        }

        [TestMethod]
        public void ScanByPrefix_ReturnsExampleObjects()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Face.Stub(s => s.scannerOpenWithPrefix(Arg<byte[]>.Is.Anything, Arg<byte[]>.Is.Anything, Arg<List<byte[]>>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything)).Return(10).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(GetReturnResultsForExampleObject()).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(new List<TRowResult>()).Repeat.Once();

                IDictionary<string, ExampleObject> ExamplesDictionary = Client.ScanByPrefix<ExampleObject>(TABLE, COLUMNFAMILY, KEY);

                AssertEqualsFilledExampleObjectIDictionary(ExamplesDictionary);
            }
        }

        [TestMethod]
        public void Scan_ReturnsExampleObjects()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Face.Stub(s => s.scannerOpen(Arg<byte[]>.Is.Anything, Arg<byte[]>.Is.Anything, Arg<List<byte[]>>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything)).Return(10).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(GetReturnResultsForExampleObject()).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(new List<TRowResult>()).Repeat.Once();

                IDictionary<string, ExampleObject> ExamplesDictionary = Client.Scan<ExampleObject>(TABLE, COLUMNFAMILY, KEY);

                AssertEqualsFilledExampleObjectIDictionary(ExamplesDictionary);
            }
        }

        [TestMethod]
        public void Scan_ReturnsExampleObjectsWithStop()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Face.Stub(s => s.scannerOpenWithStop(Arg<byte[]>.Is.Anything, Arg<byte[]>.Is.Anything, Arg<byte[]>.Is.Anything, Arg<List<byte[]>>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything))
                    .Return(10).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(GetReturnResultsForExampleObject()).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(new List<TRowResult>()).Repeat.Once();

                IDictionary<string, ExampleObject> ExamplesDictionary = Client.Scan<ExampleObject>(TABLE, COLUMNFAMILY, KEY, KEY3);

                AssertEqualsFilledExampleObjectIDictionary(ExamplesDictionary);
            }
        }

        [TestMethod]
        public void Scan_ReturnsExampleObjectsSubstrings()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Face.Stub(s => s.scannerOpenWithScan(Arg<byte[]>.Is.Anything, Arg<TScan>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything)).Return(10).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(GetReturnResultsForExampleObject()).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(new List<TRowResult>()).Repeat.Once();

                IDictionary<string, ExampleObject> ExamplesDictionary = Client.Scan<ExampleObject>(TABLE, COLUMNFAMILY, new string[] { "foo", "bar" });

                AssertEqualsFilledExampleObjectIDictionary(ExamplesDictionary);
            }
        }

        [TestMethod]
        public void Scan_ReturnsExampleObjectsPrefixes()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Face.Stub(s => s.scannerOpenWithScan(Arg<byte[]>.Is.Anything, Arg<TScan>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything)).Return(10).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(GetReturnResultsForExampleObject()).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(new List<TRowResult>()).Repeat.Once();

                IDictionary<string, ExampleObject> ExamplesDictionary = Client.Scan<ExampleObject>(TABLE, COLUMNFAMILY, null, new string[] { "foo", "bar" });

                AssertEqualsFilledExampleObjectIDictionary(ExamplesDictionary);
            }
        }

        [TestMethod]
        public void Scan_ReturnsExampleObjectsRegex()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Face.Stub(s => s.scannerOpenWithScan(Arg<byte[]>.Is.Anything, Arg<TScan>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything)).Return(10).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(GetReturnResultsForExampleObject()).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(new List<TRowResult>()).Repeat.Once();

                IDictionary<string, ExampleObject> ExamplesDictionary = Client.Scan<ExampleObject>(TABLE, COLUMNFAMILY, null, null, "^foo.");

                AssertEqualsFilledExampleObjectIDictionary(ExamplesDictionary);
            }
        }

        [TestMethod]
        public void ScanByRegex_ReturnsExampleObjects()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Face.Stub(s => s.scannerOpenWithScan(Arg<byte[]>.Is.Anything, Arg<TScan>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything)).Return(10).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(GetReturnResultsForExampleObject()).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(new List<TRowResult>()).Repeat.Once();

                IDictionary<string, ExampleObject> ExamplesDictionary = Client.ScanByRegex<ExampleObject>(TABLE, COLUMNFAMILY, "^foo.");

                AssertEqualsFilledExampleObjectIDictionary(ExamplesDictionary);
            }
        }

        [TestMethod]
        public void ScanByRegex_ReturnsExampleObjectsWithStart()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Face.Stub(s => s.scannerOpenWithScan(Arg<byte[]>.Is.Anything, Arg<TScan>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything)).Return(10).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(GetReturnResultsForExampleObject()).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(new List<TRowResult>()).Repeat.Once();

                IDictionary<string, ExampleObject> ExamplesDictionary = Client.ScanByRegex<ExampleObject>(TABLE, COLUMNFAMILY, KEY, "^foo.");

                AssertEqualsFilledExampleObjectIDictionary(ExamplesDictionary);
            }
        }

        [TestMethod]
        public void ScanByRegex_ReturnsExampleObjectsWithStop()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Face.Stub(s => s.scannerOpenWithScan(Arg<byte[]>.Is.Anything, Arg<TScan>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything)).Return(10).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(GetReturnResultsForExampleObject()).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(new List<TRowResult>()).Repeat.Once();

                IDictionary<string, ExampleObject> ExamplesDictionary = Client.ScanByRegex<ExampleObject>(TABLE, COLUMNFAMILY, KEY, KEY2, "^foo.");

                AssertEqualsFilledExampleObjectIDictionary(ExamplesDictionary);
            }
        }

        [TestMethod]
        public void ScanByPrefixesWithRegex_ReturnsExampleObjects()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Face.Stub(s => s.scannerOpenWithScan(Arg<byte[]>.Is.Anything, Arg<TScan>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything)).Return(10).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(GetReturnResultsForExampleObject()).Repeat.Once();
                Face.Stub(s => s.scannerGetList(Arg<int>.Is.Anything, Arg<int>.Is.Anything)).Return(new List<TRowResult>()).Repeat.Once();

                IDictionary<string, ExampleObject> ExamplesDictionary = Client.ScanByPrefixesWithRegex<ExampleObject>(TABLE, COLUMNFAMILY, new string[] { "foo", "bar" }, "^foo.");

                AssertEqualsFilledExampleObjectIDictionary(ExamplesDictionary);
            }
        }

        [TestMethod]
        public void AtomicIncrement_CallsClientCorrectly()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Client.AtomicIncrement(TABLE, KEY, COLUMNFAMILY, COLUMN, ATOMICVALUE);
                IList<object[]> Args = Face.GetArgumentsForCallsMadeOn(s => s.atomicIncrement(Arg<byte[]>.Is.Anything, Arg<byte[]>.Is.Anything,
                    Arg<byte[]>.Is.Anything, Arg<long>.Is.Anything));

                Assert.AreEqual(1, Args.Count);

                AssertEquivalentByteArray(Args.First().First(), TABLE);
                AssertEquivalentByteArray(Args.First()[1], KEY);
                AssertEquivalentByteArray(Args.First()[2], COLUMNFAMILY + ":" + COLUMN);

                Assert.AreEqual(Args.First()[3], ATOMICVALUE);
            }
        }

        [TestMethod]
        public void AtomicIncrementWithTimeStamp_CallsClientCorrectly()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Client.AtomicIncrement(TABLE, KEY, COLUMNFAMILY, ATOMICVALUE, TimeStamp, INTERVAL);
                IList<object[]> Args = Face.GetArgumentsForCallsMadeOn(s => s.atomicIncrement(Arg<byte[]>.Is.Anything, Arg<byte[]>.Is.Anything,
                    Arg<byte[]>.Is.Anything, Arg<long>.Is.Anything));

                Assert.AreEqual(1, Args.Count);

                AssertEquivalentByteArray(Args.First().First(), TABLE);
                AssertEquivalentByteArray(Args.First()[1], KEY);
                CollectionAssert.AreEqual((byte[])Args.First()[2], ClientEncoder.GetColumnFamilyColumnNameByteArray(COLUMNFAMILY, TimeStamp, INTERVAL));

                Assert.AreEqual(Args.First()[3], ATOMICVALUE);
            }
        }

        [TestMethod]
        public void BulkAtomicIncrement_CallsClientCorrectly()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                IList<IAtomicIncrement> Increments = new List<IAtomicIncrement>()
                {
                    new AtomicIncrement(TABLE, KEY, COLUMNFAMILY, COLUMN, ATOMICVALUE),
                    new AtomicIncrement(TABLE, KEY, COLUMNFAMILY, COLUMN, ATOMICVALUE)
                };

                Client.BulkAtomicIncrement(Increments);

                IList<object[]> Args = Face.GetArgumentsForCallsMadeOn(s => s.incrementRows(Arg<List<TIncrement>>.Is.Anything));

                Assert.AreEqual(2, ((IList<TIncrement>)(Args.First().First())).Count());

                AssertEquivalentByteArray(((List<TIncrement>)Args.First().First()).First().Table, TABLE);
                AssertEquivalentByteArray(((List<TIncrement>)Args.First().First()).First().Row, KEY);
                AssertEquivalentByteArray(((List<TIncrement>)Args.First().First()).First().Column, COLUMNFAMILY + ":" + COLUMN);
            }
        }

        [TestMethod]
        public void DeleteRow_CallsClientCorrectly()
        {
            Hbase.Hbase.Iface Face = MockRepository.GenerateStub<Hbase.Hbase.Iface>();
            using (Client Client = new Client(GetMockClientPool(Face)))
            {
                Client.DeleteRow(TABLE, KEY);

                IList<object[]> Args = Face.GetArgumentsForCallsMadeOn(s => s.deleteAllRow(Arg<byte[]>.Is.Anything, Arg<byte[]>.Is.Anything,
                    Arg<Dictionary<byte[], byte[]>>.Is.Anything));

                AssertEquivalentByteArray(Args.First().First(), TABLE);
                AssertEquivalentByteArray(Args.First()[1], KEY);
            }
        }

        [Ignore, TestMethod]
        public void DoActualWork()
        {
            using (Client Client = new Client(SERVER, 9090, 1000000))
            {
                Client.PutOne("test", "test", "test", GetFilledExampleObject());
            }
        }

        #region Static Internals
        private static IHBaseClientPool GetMockClientPool(Hbase.Hbase.Iface Client)
        {
            IPooledHBaseClient PooledClient = new TestPooledHBaseClient(Client);
            IHBaseClientPool ClientPool = new TestHBaseClientPool(PooledClient);

            return ClientPool;
        }

        private static void AssertEquivalentByteArray(object Actual, string Expected)
        {
            CollectionAssert.AreEquivalent(ClientEncoder.EncodeString(Expected), (byte[])Actual);
        }

        private static ExampleObject GetFilledExampleObject()
        {
            return new ExampleObject()
            {
                IntField = INTFIELD,
                IntProperty = INTPROPERTY,
                DateTimeField = DateTime.MinValue,
                DateTimeProperty = DateTime.MaxValue,
                StringField = STRINGFIELD,
                StringProperty = STRINGPROPERTY
            };
        }

        private static ExampleNullableObject GetFilledExampleNullableObject()
        {
            return new ExampleNullableObject()
            {
                IntField = INTFIELD,
                IntProperty = INTPROPERTY,
                DateTimeField = DateTime.MinValue,
                DateTimeProperty = DateTime.MaxValue,
                StringField = STRINGFIELD,
                StringProperty = STRINGPROPERTY,
                NullableDate = DateTime.MaxValue,
                NullableBoolean = true
            };
        }

        private static Dictionary<byte[], TCell> GetReturnDictionaryForExampleObject()
        {
            Dictionary<byte[], TCell> ReturnDictionary = GetReturnDictionaryForIExampleObject();

            ReturnDictionary.Add(ClientEncoder.EncodeString(COLUMNFAMILY + ":IntField"), new TCell() { Value = Reverse(BitConverter.GetBytes(INTFIELD)) });
            ReturnDictionary.Add(ClientEncoder.EncodeString(COLUMNFAMILY + ":StringField"), new TCell() { Value = ClientEncoder.EncodeString(STRINGFIELD) });
            ReturnDictionary.Add(ClientEncoder.EncodeString(COLUMNFAMILY + ":DateTimeField"), new TCell()
            {
                Value = ClientEncoder.EncodeString(DateTime.MinValue.ToString(ClientEncoder.DATEFORMAT) + ".0")
            });

            return ReturnDictionary;
        }

        private static List<TRowResult> GetReturnResultsForExampleObject()
        {
            List<TRowResult> ReturnResults = new List<TRowResult>();

            ReturnResults.Add(GetTRowResult(KEY));
            ReturnResults.Add(GetTRowResult(KEY2));
            ReturnResults.Add(GetTRowResult(KEY3));

            return ReturnResults;
        }

        private static TRowResult GetTRowResult(string Key)
        {
            return new TRowResult()
            {
                Columns = GetReturnDictionaryForExampleObject(),
                Row = ClientEncoder.EncodeString(Key)
            };
        }

        private static void AssertEqualsFilledExampleObject(ExampleObject Example)
        {
            Assert.AreEqual(INTFIELD, Example.IntField);
            Assert.AreEqual(INTPROPERTY, Example.IntProperty);
            Assert.AreEqual(STRINGFIELD, Example.StringField);
            Assert.AreEqual(STRINGPROPERTY, Example.StringProperty);
            Assert.AreEqual(DateTime.MinValue, Example.DateTimeField);
            Assert.AreEqual(DateTime.MaxValue.ToString(ClientEncoder.DATEFORMAT), Example.DateTimeProperty.ToString(ClientEncoder.DATEFORMAT));
        }

        private static void AssertEqualsFilledExampleObjectIDictionary(IDictionary<string, ExampleObject> ExamplesDictionary)
        {
            AssertEqualsFilledExampleObject(ExamplesDictionary[KEY]);
            AssertEqualsFilledExampleObject(ExamplesDictionary[KEY2]);
            AssertEqualsFilledExampleObject(ExamplesDictionary[KEY3]);

            Assert.AreEqual(3, ExamplesDictionary.Count);
        }

        private static Dictionary<byte[], TCell> GetReturnDictionaryForIExampleObject()
        {
            Dictionary<byte[], TCell> ReturnDictionary = new Dictionary<byte[], TCell>();

            ReturnDictionary.Add(ClientEncoder.EncodeString(COLUMNFAMILY + ":IntProperty"), new TCell()
            {
                Value = Reverse(BitConverter.GetBytes(INTPROPERTY))
            });

            ReturnDictionary.Add(ClientEncoder.EncodeString(COLUMNFAMILY + ":StringProperty"), new TCell()
            {
                Value = ClientEncoder.EncodeString(STRINGPROPERTY)
            });

            ReturnDictionary.Add(ClientEncoder.EncodeString(COLUMNFAMILY + ":DateTimeProperty"), new TCell()
            {
                Value = ClientEncoder.EncodeString(DateTime.MaxValue.ToString(ClientEncoder.DATEFORMAT))
            });

            return ReturnDictionary;
        }

        private static Dictionary<byte[], TCell> GetTimeSeriesReturnDictionaryForIExampleObject(DateTime TimeStamp)
        {
            Dictionary<byte[], TCell> ReturnDictionary = new Dictionary<byte[], TCell>();

            ReturnDictionary.Add(ClientEncoder.GetColumnFamilyColumnNameByteArray("IntProperty", TimeStamp, INTERVAL), new TCell()
            {
                Value = Reverse(BitConverter.GetBytes(INTPROPERTY))
            });

            ReturnDictionary.Add(ClientEncoder.GetColumnFamilyColumnNameByteArray("StringProperty", TimeStamp, INTERVAL), new TCell()
            {
                Value = ClientEncoder.EncodeString(STRINGPROPERTY)
            });

            ReturnDictionary.Add(ClientEncoder.GetColumnFamilyColumnNameByteArray("DateTimeProperty", TimeStamp, INTERVAL), new TCell()
            {
                Value = ClientEncoder.EncodeString(DateTime.MaxValue.ToString(ClientEncoder.DATEFORMAT))
            });

            return ReturnDictionary;
        }

        private static Dictionary<byte[], TCell> GetTimeSeriesRangeReturnDictionaryForIExampleObject()
        {
            Dictionary<byte[], TCell> ReturnDictionary = GetTimeSeriesReturnDictionaryForIExampleObject(StartTime);

            foreach (KeyValuePair<byte[], TCell> kvp in GetTimeSeriesReturnDictionaryForIExampleObject(StopTime))
            {
                ReturnDictionary.Add(kvp.Key, kvp.Value);
            }

            return ReturnDictionary;
        }

        private static byte[] Reverse(byte[] Input)
        {
            if ((object)Input != null)
            {
                Array.Reverse(Input);
            }

            return Input;
        }

        private static void AssertExpectedMutationsForExampleNullableObject(IList<Mutation> Mutations, bool IsDeleted)
        {
            AssertContainsExpectedMutationValue(COLUMNFAMILY + ":NullableDate", DateTime.MaxValue.ToString(ClientEncoder.DATEFORMAT), Mutations, IsDeleted);
            AssertContainsExpectedMutationValue(COLUMNFAMILY + ":NullableBoolean", true, Mutations, IsDeleted);

            AssertExpectedMutationsForExampleObject(Mutations, IsDeleted);
        }

        private static void AssertExpectedMutationsForExampleObject(IList<Mutation> Mutations, bool IsDeleted)
        {
            AssertContainsExpectedMutationValue(COLUMNFAMILY + ":IntField", INTFIELD, Mutations, IsDeleted);
            AssertContainsExpectedMutationValue(COLUMNFAMILY + ":StringField", STRINGFIELD, Mutations, IsDeleted);
            AssertContainsExpectedMutationValue(COLUMNFAMILY + ":DateTimeField", DateTime.MinValue.ToString(ClientEncoder.DATEFORMAT) + ".0",
                Mutations, IsDeleted);

            AssertExpectedMutationsForIExampleObject(Mutations, IsDeleted);
        }

        private static void AssertExpectedMutationsForIExampleObject(IList<Mutation> Mutations, bool IsDeleted)
        {
            AssertContainsExpectedMutationValue(COLUMNFAMILY + ":IntProperty", INTPROPERTY, Mutations, IsDeleted);
            AssertContainsExpectedMutationValue(COLUMNFAMILY + ":StringProperty", STRINGPROPERTY, Mutations, IsDeleted);
            AssertContainsExpectedMutationValue(COLUMNFAMILY + ":DateTimeProperty", DateTime.MaxValue.ToString(ClientEncoder.DATEFORMAT), Mutations, IsDeleted);
        }

        private static void AssertContainsExpectedMutationValue(string ColumnName, bool Expected, IList<Mutation> Mutations, bool IsDeleted)
        {
            if (IsDeleted)
            {
                AssertMutationIsDeleted(ColumnName, Mutations);
            }
            else
            {
                CollectionAssert.AreEquivalent(new byte[] { Convert.ToByte(Expected) },
                    Mutations.Where(m => Enumerable.SequenceEqual(m.Column, ClientEncoder.EncodeString(ColumnName))).FirstOrDefault().Value);
            }
        }

        private static void AssertContainsExpectedMutationValue(string ColumnName, string Expected, IList<Mutation> Mutations, bool IsDeleted)
        {
            if (IsDeleted)
            {
                AssertMutationIsDeleted(ColumnName, Mutations);
            }
            else
            {
                CollectionAssert.AreEquivalent(ClientEncoder.EncodeString(Expected),
                    Mutations.Where(m => Enumerable.SequenceEqual(m.Column, ClientEncoder.EncodeString(ColumnName))).FirstOrDefault().Value);
            }
        }

        private static void AssertContainsExpectedMutationValue(string ColumnName, int Expected, IList<Mutation> Mutations, bool IsDeleted)
        {
            if (IsDeleted)
            {
                AssertMutationIsDeleted(ColumnName, Mutations);
            }
            else
            {
                CollectionAssert.AreEquivalent(Reverse(BitConverter.GetBytes(Expected)),
                    Mutations.Where(m => Enumerable.SequenceEqual(m.Column, ClientEncoder.EncodeString(ColumnName))).FirstOrDefault().Value);
            }
        }

        private static void AssertMutationIsDeleted(string ColumnName, IList<Mutation> Mutations)
        {
            Assert.IsTrue(Mutations.Where(m => Enumerable.SequenceEqual(m.Column, ClientEncoder.EncodeString(ColumnName))).FirstOrDefault().IsDelete);
        }
        #endregion

        #region Example Structures
        private class ExampleObject
            : IExampleObject
        {
            private int _IntProperty;
            private string _StringProperty;
            private DateTime _DateTimeProperty;

            public int IntField;
            public string StringField;
            public DateTime DateTimeField;

            public int IntProperty
            {
                get
                {
                    return _IntProperty;
                }
                set
                {
                    _IntProperty = value;
                }
            }

            public string StringProperty
            {
                get
                {
                    return _StringProperty;
                }
                set
                {
                    _StringProperty = value;
                }
            }

            public DateTime DateTimeProperty
            {
                get
                {
                    return _DateTimeProperty;
                }
                set
                {
                    _DateTimeProperty = value;
                }
            }

            public void MethodShouldNotBeCalled()
            {
                throw new NotImplementedException("The method on the ExampleObject should never be called!");
            }
        }

        private class ExampleNullableObject
            : ExampleObject
        {
            public DateTime? NullableDate;
            public bool? NullableBoolean;
        }

        internal interface IExampleObject
        {
            int IntProperty
            {
                get;
                set;
            }

            string StringProperty
            {
                get;
                set;
            }

            DateTime DateTimeProperty
            {
                get;
                set;
            }
        }
        #endregion
    }
}