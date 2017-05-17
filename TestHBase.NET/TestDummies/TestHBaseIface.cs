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
using System.Net.Sockets;
using System.IO;
using Thrift.Transport;
using Thrift;
using Hbase;
using System.Threading;

namespace TestHBase.NET.TestDummies
{
    public class TestHBaseIface : Hbase.Hbase.Iface
    {
        private int _OperationCounter;
        public int OperationCounter { get { return this._OperationCounter; } }

        public HBaseErrorContinuity ErrorContinuity { get; set; }
        public HBaseErrorType ErrorType { get; set; }

        private static Random _Random = new Random();

        public TestHBaseIface(HBaseErrorContinuity errorContinuity, HBaseErrorType errorType)
        {
            this.ErrorContinuity = errorContinuity;
            this.ErrorType = errorType;
        }

        private T Execute<T>()
        {
            try
            {
                Exception ex;
                switch (ErrorContinuity)
                {
                    case HBaseErrorContinuity.Never:
                        return default(T);

                    case HBaseErrorContinuity.Once:
                        if (this._OperationCounter < 1)
                            goto case HBaseErrorContinuity.Always;
                        break;

                    case HBaseErrorContinuity.Twice:
                        if (this._OperationCounter < 2)
                            goto case HBaseErrorContinuity.Always;
                        break;

                    case HBaseErrorContinuity.EveryTwo:
                        if (this._OperationCounter % 4 < 2)
                            goto case HBaseErrorContinuity.Always;
                        break;

                    case HBaseErrorContinuity.Always:
                        switch (this.ErrorType)
                        {
                            case HBaseErrorType.Application:
                                throw new ApplicationException();

                            case HBaseErrorType.Network:
                                ex = new SocketException((int)SocketError.TimedOut);
                                if (this._OperationCounter % 3 == 0) ex = new IOException("", new SocketException());
                                if (this._OperationCounter % 3 == 1) ex = new TTransportException();
                                throw ex;

                            case HBaseErrorType.HBase:
                                ex = new TApplicationException();
                                if (this._OperationCounter % 2 == 0) ex = new IOError();
                                throw ex;
                        }
                        break;
                }

                return default(T);
            }
            catch
            {
                // delay randomly on any error between 0s and 10s
                Thread.Sleep(_Random.Next(1500));
                throw;
            }
            finally
            {
                // all operations take anywhere from 0 to 500ms
                Thread.Sleep(_Random.Next(500));
                Interlocked.Increment(ref this._OperationCounter);
            }
        }

        public void enableTable(byte[] tableName)
        {
            Execute<int>();
        }

        public void disableTable(byte[] tableName)
        {
            Execute<int>();
        }

        public bool isTableEnabled(byte[] tableName)
        {
            return Execute<bool>();
        }

        public void compact(byte[] tableNameOrRegionName)
        {
            Execute<int>();
        }

        public void majorCompact(byte[] tableNameOrRegionName)
        {
            Execute<int>();
        }

        public List<byte[]> getTableNames()
        {
            return Execute<List<byte[]>>();
        }

        public Dictionary<byte[], Hbase.ColumnDescriptor> getColumnDescriptors(byte[] tableName)
        {
            return Execute<Dictionary<byte[], Hbase.ColumnDescriptor>>();
        }

        public List<Hbase.TRegionInfo> getTableRegions(byte[] tableName)
        {
            return Execute<List<Hbase.TRegionInfo>>();
        }

        public void createTable(byte[] tableName, List<Hbase.ColumnDescriptor> columnFamilies)
        {
            Execute<int>();
        }

        public void deleteTable(byte[] tableName)
        {
            Execute<int>();
        }

        public List<Hbase.TCell> get(byte[] tableName, byte[] row, byte[] column, Dictionary<byte[], byte[]> attributes)
        {
            return Execute<List<Hbase.TCell>>();
        }

        public List<Hbase.TCell> getVer(byte[] tableName, byte[] row, byte[] column, int numVersions, Dictionary<byte[], byte[]> attributes)
        {
            return Execute<List<Hbase.TCell>>();
        }

        public List<Hbase.TCell> getVerTs(byte[] tableName, byte[] row, byte[] column, long timestamp, int numVersions, Dictionary<byte[], byte[]> attributes)
        {
            return Execute<List<Hbase.TCell>>();
        }

        public List<Hbase.TRowResult> getRow(byte[] tableName, byte[] row, Dictionary<byte[], byte[]> attributes)
        {
            return Execute<List<Hbase.TRowResult>>();
        }

        public List<Hbase.TRowResult> getRowWithColumns(byte[] tableName, byte[] row, List<byte[]> columns, Dictionary<byte[], byte[]> attributes)
        {
            return Execute<List<Hbase.TRowResult>>();
        }

        public List<Hbase.TRowResult> getRowTs(byte[] tableName, byte[] row, long timestamp, Dictionary<byte[], byte[]> attributes)
        {
            return Execute<List<Hbase.TRowResult>>();
        }

        public List<Hbase.TRowResult> getRowWithColumnsTs(byte[] tableName, byte[] row, List<byte[]> columns, long timestamp, Dictionary<byte[], byte[]> attributes)
        {
            return Execute<List<Hbase.TRowResult>>();
        }

        public List<Hbase.TRowResult> getRows(byte[] tableName, List<byte[]> rows, Dictionary<byte[], byte[]> attributes)
        {
            return Execute<List<Hbase.TRowResult>>();
        }

        public List<Hbase.TRowResult> getRowsWithColumns(byte[] tableName, List<byte[]> rows, List<byte[]> columns, Dictionary<byte[], byte[]> attributes)
        {
            return Execute<List<Hbase.TRowResult>>();
        }

        public List<Hbase.TRowResult> getRowsTs(byte[] tableName, List<byte[]> rows, long timestamp, Dictionary<byte[], byte[]> attributes)
        {
            return Execute<List<Hbase.TRowResult>>();
        }

        public List<Hbase.TRowResult> getRowsWithColumnsTs(byte[] tableName, List<byte[]> rows, List<byte[]> columns, long timestamp, Dictionary<byte[], byte[]> attributes)
        {
            return Execute<List<Hbase.TRowResult>>();
        }

        public void mutateRow(byte[] tableName, byte[] row, List<Hbase.Mutation> mutations, Dictionary<byte[], byte[]> attributes)
        {
            Execute<int>();
        }

        public void mutateRowTs(byte[] tableName, byte[] row, List<Hbase.Mutation> mutations, long timestamp, Dictionary<byte[], byte[]> attributes)
        {
            Execute<int>();
        }

        public void mutateRows(byte[] tableName, List<Hbase.BatchMutation> rowBatches, Dictionary<byte[], byte[]> attributes)
        {
            Execute<int>();
        }

        public void mutateRowsTs(byte[] tableName, List<Hbase.BatchMutation> rowBatches, long timestamp, Dictionary<byte[], byte[]> attributes)
        {
            Execute<int>();
        }

        public long atomicIncrement(byte[] tableName, byte[] row, byte[] column, long value)
        {
            return Execute<long>();
        }

        public void deleteAll(byte[] tableName, byte[] row, byte[] column, Dictionary<byte[], byte[]> attributes)
        {
            Execute<int>();
        }

        public void deleteAllTs(byte[] tableName, byte[] row, byte[] column, long timestamp, Dictionary<byte[], byte[]> attributes)
        {
            Execute<int>();
        }

        public void deleteAllRow(byte[] tableName, byte[] row, Dictionary<byte[], byte[]> attributes)
        {
            Execute<int>();
        }

        public void increment(Hbase.TIncrement increment)
        {
            Execute<int>();
        }

        public void incrementRows(List<Hbase.TIncrement> increments)
        {
            Execute<int>();
        }

        public void deleteAllRowTs(byte[] tableName, byte[] row, long timestamp, Dictionary<byte[], byte[]> attributes)
        {
            Execute<int>();
        }

        public int scannerOpenWithScan(byte[] tableName, Hbase.TScan scan, Dictionary<byte[], byte[]> attributes)
        {
            return Execute<int>();
        }

        public int scannerOpen(byte[] tableName, byte[] startRow, List<byte[]> columns, Dictionary<byte[], byte[]> attributes)
        {
            return Execute<int>();
        }

        public int scannerOpenWithStop(byte[] tableName, byte[] startRow, byte[] stopRow, List<byte[]> columns, Dictionary<byte[], byte[]> attributes)
        {
            return Execute<int>();
        }

        public int scannerOpenWithPrefix(byte[] tableName, byte[] startAndPrefix, List<byte[]> columns, Dictionary<byte[], byte[]> attributes)
        {
            return Execute<int>();
        }

        public int scannerOpenTs(byte[] tableName, byte[] startRow, List<byte[]> columns, long timestamp, Dictionary<byte[], byte[]> attributes)
        {
            return Execute<int>();
        }

        public int scannerOpenWithStopTs(byte[] tableName, byte[] startRow, byte[] stopRow, List<byte[]> columns, long timestamp, Dictionary<byte[], byte[]> attributes)
        {
            return Execute<int>();
        }

        public List<Hbase.TRowResult> scannerGet(int id)
        {
            return Execute<List<Hbase.TRowResult>>();
        }

        public List<Hbase.TRowResult> scannerGetList(int id, int nbRows)
        {
            return Execute<List<Hbase.TRowResult>>();
        }

        public void scannerClose(int id)
        {
            Execute<int>();
        }

        public List<Hbase.TCell> getRowOrBefore(byte[] tableName, byte[] row, byte[] family)
        {
            return Execute<List<Hbase.TCell>>();
        }

        public Hbase.TRegionInfo getRegionInfo(byte[] row)
        {
            return Execute<Hbase.TRegionInfo>();
        }

        public List<TCell> append(TAppend append)
        {
            return Execute<List<TCell>>();
        }

        public bool checkAndPut(byte[] tableName, byte[] row, byte[] column, byte[] value, Mutation mput, Dictionary<byte[], byte[]> attributes)
        {
            return Execute<bool>();
        }
    }
}
