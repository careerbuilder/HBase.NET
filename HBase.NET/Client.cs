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
using System.Reflection;
using Castle.DynamicProxy;
using System.Globalization;
using System.IO;
using Hbase.StaticInternals;

namespace Hbase
{
    public class Client
        : IClient
    {
        private readonly IHBaseClientPool _ClientPool;
        private const string DEFAULTCANARYTABLE = "hbase:meta";
        private const int DEFAULTNUMCONNECTIONS = 10;
        private const int DEFAULTNUMSCANROWS = 1000; //Load data in chunks of 1000 results
        private const int DEFAULTTIMEOUT = 20000;
        private const bool DEFAULTUSECOMPACTPROTOCOL = false;
        private const bool DEFAULTUSEFRAMEDTRANSPORT = false;

        #region Constructors
        internal Client(IHBaseClientPool ClientPool)
        {
            _ClientPool = ClientPool;
        }

        public Client(string Host, int Port, int BufferSize, int NumConnections = DEFAULTNUMCONNECTIONS, int Timeout = DEFAULTTIMEOUT, string CanaryTable = DEFAULTCANARYTABLE,
            bool UseCompactProtocol = DEFAULTUSECOMPACTPROTOCOL, bool UseFramedTransport = DEFAULTUSEFRAMEDTRANSPORT)
            : this(new HBaseHost[] { new HBaseHost(Host, Port) }, BufferSize, NumConnections, Timeout, CanaryTable, UseCompactProtocol, UseFramedTransport) { }

        public Client(IEnumerable<HBaseHost> Hosts, int BufferSize, int NumConnections = DEFAULTNUMCONNECTIONS, int timeout = DEFAULTTIMEOUT, string CanaryTable = DEFAULTCANARYTABLE,
            bool UseCompactProtocol = DEFAULTUSECOMPACTPROTOCOL, bool UseFramedTransport = DEFAULTUSEFRAMEDTRANSPORT)
        {
            Logger.Log.Info("Initializing HBase client.");
            _ClientPool = new HBaseClientPool(Hosts, BufferSize, NumConnections, timeout, CanaryTable, UseCompactProtocol, UseFramedTransport);
        }

        #endregion

        public int AvailableClients
        {
            get { return this._ClientPool.AvailableClients; }
        }

        public int BlockedRequests
        {
            get { return this._ClientPool.BlockedRequests; }
        }

        public int TotalClients
        {
            get { return this._ClientPool.TotalClients; }
        }

        #region Put
        public void PutOne<POCO>(string TableName, string Key, string ColumnFamily, POCO Value)
            where POCO : class
        {
            List<Mutation> Mutations = ClientMutator.GetMutations<POCO>(ColumnFamily, Value, false);

            _ClientPool.Execute(cp =>
            {
                cp.Execute(c => c.mutateRow(ClientEncoder.EncodeString(TableName), ClientEncoder.EncodeString(Key), Mutations,
                    new Dictionary<byte[], byte[]>()));
            });
        }

        public void PutBulk(string TableName, IEnumerable<IHBasePut> Bulk)
        {
            List<BatchMutation> BatchMutations = ClientMutator.GetBatchMutations(Bulk.Cast<IHBaseMutation>());

            _ClientPool.Execute(cp =>
            {
                cp.Execute(c => c.mutateRows(ClientEncoder.EncodeString(TableName), BatchMutations, new Dictionary<byte[], byte[]>()));
            });
        }
        #endregion

        #region Delete
        public void DeleteOne<POCO>(string TableName, string Key, string ColumnFamily)
            where POCO : class
        {
            List<Mutation> Mutations = ClientMutator.GetMutations<POCO>(ColumnFamily, null, true);

            _ClientPool.Execute(cp =>
            {
                cp.Execute(c => c.mutateRow(ClientEncoder.EncodeString(TableName), ClientEncoder.EncodeString(Key), Mutations, new Dictionary<byte[], byte[]>()));
            });
        }

        public void DeleteBulk(string TableName, IEnumerable<IHBaseDeletion> Bulk)
        {
            List<BatchMutation> BatchMutations = ClientMutator.GetBatchMutations(Bulk.Cast<IHBaseMutation>());

            _ClientPool.Execute(cp =>
            {
                cp.Execute(c => c.mutateRows(ClientEncoder.EncodeString(TableName), BatchMutations, new Dictionary<byte[], byte[]>()));
            });
        }

        public void DeleteRow(string TableName, string Key)
        {
            _ClientPool.Execute(cp =>
            {
                cp.Execute(c => c.deleteAllRow(ClientEncoder.EncodeString(TableName), ClientEncoder.EncodeString(Key), new Dictionary<byte[], byte[]>()));
            });
        }
        #endregion

        #region Get
        public POCO GetOne<POCO>(string TableName, string Key, string ColumnFamily)
            where POCO : class
        {
            List<byte[]> Columns = ClientReflector.GetColumns<POCO>(ColumnFamily);

            return GetOne<POCO>(TableName, Key, Columns, false);
        }

        public POCO GetOne<POCO>(string TableName, string Key, DateTime TimeStamp, TimeInterval Interval)
            where POCO : class
        {
            List<byte[]> Columns = ClientReflector.GetColumns<POCO>(TimeStamp, Interval);

            return GetOne<POCO>(TableName, Key, Columns, true);
        }

        private POCO GetOne<POCO>(string TableName, string Key, List<byte[]> Columns, bool IsTimeSeries)
            where POCO : class
        {
            TRowResult Result = GetRowResult<POCO>(TableName, Key, Columns);

            POCO Proxy = null;

            if ((object)Result != null)
            {
                Proxy = ClientReflector.GetProxy<POCO>(Result.Columns, IsTimeSeries);
            }

            return Proxy;
        }

        public IList<IHBaseCell> GetRow(string TableName, string Key, IDictionary<string, string> Attributes)
        {
            IList<IHBaseCell> ReturnList = new List<IHBaseCell>();

            TRowResult RowResult = GetRowResult(TableName, Key, Attributes);

            if ((object)RowResult != null)
            {
                foreach (KeyValuePair<byte[], TCell> Column in RowResult.Columns)
                {
                    ReturnList.Add(new HBaseCell(
                        ClientEncoder.GetColumnFamily(Column.Key),
                        ClientEncoder.GetColumnNameByteArray(Column.Key),
                        Column.Value.Value));
                }
            }

            return ReturnList;
        }

        public IList<IHBaseCell<ReturnType>> GetRow<ReturnType>(string TableName, string Key, IDictionary<string, string> Attributes)
        {
            IList<IHBaseCell<ReturnType>> ReturnList = new List<IHBaseCell<ReturnType>>();

            TRowResult RowResult = GetRowResult(TableName, Key, Attributes);

            if ((object)RowResult != null)
            {
                foreach (KeyValuePair<byte[], TCell> Column in RowResult.Columns)
                {
                    ReturnList.Add(new HBaseCell<ReturnType>(
                        ClientEncoder.GetColumnFamily(Column.Key),
                        ClientEncoder.GetColumnNameByteArray(Column.Key),
                        Column.Value.Value));
                }
            }

            return ReturnList;
        }

        public IList<IHBaseCell<ReturnType, ColumnType>> GetRow<ReturnType, ColumnType>(string TableName, string Key,
            IDictionary<string, string> Attributes)
        {
            IList<IHBaseCell<ReturnType, ColumnType>> ReturnList = new List<IHBaseCell<ReturnType, ColumnType>>();

            TRowResult RowResult = GetRowResult(TableName, Key, Attributes);

            if ((object)RowResult != null)
            {
                foreach (KeyValuePair<byte[], TCell> Column in RowResult.Columns)
                {
                    ReturnList.Add(new HBaseCell<ReturnType, ColumnType>(
                        ClientEncoder.GetColumnFamily(Column.Key),
                        ClientEncoder.GetColumnNameByteArray(Column.Key),
                        Column.Value.Value));
                }
            }

            return ReturnList;
        }

        private TRowResult GetRowResult(string TableName, string Key, IDictionary<string, string> Attributes)
        {
            IList<TRowResult> RowResults;
            Dictionary<byte[], byte[]> EncodedAttributes = new Dictionary<byte[], byte[]>();

            foreach (KeyValuePair<string, string> Attribute in Attributes)
            {
                EncodedAttributes.Add(ClientEncoder.EncodeString(Attribute.Key), ClientEncoder.EncodeString(Attribute.Value));
            }

            RowResults = _ClientPool.Execute(cp =>
            {
                return cp.Execute(c => c.getRow(ClientEncoder.EncodeString(TableName), ClientEncoder.EncodeString(Key), EncodedAttributes));
            });

            return RowResults.FirstOrDefault();
        }

        private IList<TRowResult> GetRowResult(string TableName, string Key, string ColumnFamily, string ColumnName, IDictionary<String, String> Attributes)
        {
            IList<TRowResult> RowResults;
            Dictionary<byte[], byte[]> EncodedAttributes = new Dictionary<byte[], byte[]>();

            foreach (KeyValuePair<string, string> Attribute in Attributes)
            {
                EncodedAttributes.Add(ClientEncoder.EncodeString(Attribute.Key), ClientEncoder.EncodeString(Attribute.Value));
            }

            RowResults = _ClientPool.Execute(cp =>
            {
                return cp.Execute(c => c.getRowWithColumns(ClientEncoder.EncodeString(TableName), ClientEncoder.EncodeString(Key), new List<byte[]> { ClientEncoder.EncodeString(ColumnFamily + ':' + ColumnName) }, EncodedAttributes));
            });

            return RowResults;
        }

        private TRowResult GetRowResult<POCO>(string TableName, string Key, List<byte[]> Columns)
        {
            IList<TRowResult> RowResults;

            RowResults = _ClientPool.Execute(cp =>
            {
                return cp.Execute(c => c.getRowWithColumns(ClientEncoder.EncodeString(TableName), ClientEncoder.EncodeString(Key), Columns, new Dictionary<byte[], byte[]>()));
            });

            return RowResults.FirstOrDefault();
        }

        private IEnumerable<TRowResult> GetRowResults<POCO>(string TableName, IEnumerable<string> Keys, List<byte[]> Columns)
        {
            IList<TRowResult> RowResults;

            RowResults = _ClientPool.Execute(cp =>
            {
                return cp.Execute(c => c.getRowsWithColumns(ClientEncoder.EncodeString(TableName), Keys.Select(Key => ClientEncoder.EncodeString(Key)).ToList(), Columns, new Dictionary<byte[], byte[]>()));
            });

            return RowResults;
        }

        public IDictionary<DateTime, POCO> GetRange<POCO>(string TableName, string Key, DateTime StartTime, DateTime StopTime, TimeInterval Interval)
            where POCO : class
        {
            List<byte[]> Columns = ClientReflector.GetColumns<POCO>(StartTime, StopTime, Interval);

            TRowResult Result = GetRowResult<POCO>(TableName, Key, Columns);

            IDictionary<DateTime, POCO> Proxies = new Dictionary<DateTime, POCO>();

            if ((object)Result != null)
            {
                Proxies = ClientReflector.GetProxies<POCO>(Result.Columns, Interval);
            }

            return Proxies;
        }

        public IDictionary<string, IDictionary<DateTime, POCO>> GetRange<POCO>(string TableName, IEnumerable<string> Keys, DateTime StartTime, DateTime StopTime, TimeInterval Interval)
            where POCO : class
        {
            List<byte[]> Columns = ClientReflector.GetColumns<POCO>(StartTime, StopTime, Interval);

            IEnumerable<TRowResult> Results = GetRowResults<POCO>(TableName, Keys, Columns);

            IDictionary<string, IDictionary<DateTime, POCO>> Proxies = new Dictionary<String, IDictionary<DateTime, POCO>>();

            foreach (var Result in Results)
            {
                var Proxy = ClientReflector.GetProxies<POCO>(Result.Columns, Interval);
                Proxies.Add(ClientEncoder.DecodeString(Result.Row), Proxy);
            }

            return Proxies;
        }

        public T GetClassCell<T>(string TableName, string Key, string ColumnFamily, string Column)
            where T : class
        {
            return GetClassCell<T, string>(TableName, Key, ColumnFamily, Column);
        }

        public T GetClassCell<T, ColumnNameType>(string TableName, string Key, string ColumnFamily, ColumnNameType Column)
            where T : class
        {
            T ReturnValue = null;
            IList<TCell> Cells = GetCellFromClient<ColumnNameType>(TableName, Key, ColumnFamily, Column);

            if (Cells.Count > 0)
            {
                ReturnValue = (T)ClientReflector.TryGetValueByType(typeof(T), Cells.First().Value);
            }

            return ReturnValue;
        }

        public T? GetStructureCell<T>(string TableName, string Key, string ColumnFamily, string Column)
            where T : struct
        {
            return GetStructureCell<T, string>(TableName, Key, ColumnFamily, Column);
        }

        public T? GetStructureCell<T, ColumnNameType>(string TableName, string Key, string ColumnFamily, ColumnNameType Column)
            where T : struct
        {
            T? ReturnValue = null;
            IList<TCell> Cells = GetCellFromClient<ColumnNameType>(TableName, Key, ColumnFamily, Column);

            if (Cells.Count > 0)
            {
                ReturnValue = (T?)ClientReflector.TryGetValueByType(typeof(T), Cells.First().Value);
            }

            return ReturnValue;
        }

        private IList<TCell> GetCellFromClient<ColumnNameType>(string TableName, string Key, string ColumnFamily, ColumnNameType Column)
        {
            return _ClientPool.Execute(cp =>
            {
                return cp.Execute(c => c.get(ClientEncoder.EncodeString(TableName), ClientEncoder.EncodeString(Key),
                    ClientEncoder.GetColumnFamilyColumnNameByteArray(typeof(ColumnNameType), ColumnFamily, Column),
                    new Dictionary<byte[], byte[]>()));
            });
        }

        public IDictionary<string, long> GetOptimizationCounts(string TableName,
            IEnumerable<IHBaseIndexOptimizationLookup> IndexOptimizationLookups)
        {
            IDictionary<string, long> ReturnDictionary = new Dictionary<string, long>();
            IDictionary<IHBaseCellInfo, object> CellsDictionary = GetCells(TableName,
                (IEnumerable<IHBaseCellInfo>)IndexOptimizationLookups);

            foreach (IHBaseIndexOptimizationLookup Lookup in IndexOptimizationLookups)
            {
                if ((object)Lookup != null)
                {
                    ReturnDictionary.Add(Lookup.ColumnFamily, (long)CellsDictionary[Lookup]);
                }
            }

            return ReturnDictionary;
        }

        public IDictionary<long, string> GetRange(string TableName, string Key, string Index, long Start, long Stop)
        {
            IDictionary<long, string> ReturnDictionary = new Dictionary<long, string>();
            IList<IHBaseClassCell<string, long>> IndexCells = new List<IHBaseClassCell<string, long>>();

            for (long i = Start; i <= Stop; ++i)
            {
                IndexCells.Add(new HBaseClassCell<string, long>(Key, Index, i));
            }

            IDictionary<IHBaseCellInfo, object> CellsDictionary = GetCells(TableName, (IEnumerable<IHBaseCellInfo>)IndexCells);

            foreach (IHBaseClassCell<string, long> Cell in IndexCells)
            {
                if ((object)Cell != null)
                {
                    ReturnDictionary.Add(Cell.Column, (string)CellsDictionary[Cell]);
                }
            }

            return ReturnDictionary;
        }

        public IList<IHBaseStructureCell<T, ColumnType>> GetCells<T, ColumnType>(string TableName,
            IEnumerable<IHBaseStructureCell<T, ColumnType>> Cells)
            where T : struct
        {
            IList<IHBaseStructureCell<T, ColumnType>> ReturnCells = new List<IHBaseStructureCell<T, ColumnType>>();
            IDictionary<IHBaseCellInfo, object> CellsDictionary = GetCells(TableName, (IEnumerable<IHBaseCellInfo>)Cells);

            foreach (IHBaseStructureCell<T, ColumnType> Cell in Cells)
            {
                if ((object)Cell != null)
                {
                    Cell.Value = (T?)CellsDictionary[Cell];
                }

                ReturnCells.Add(Cell);
            }

            return ReturnCells;
        }

        public IList<IHBaseClassCell<T, ColumnType>> GetCells<T, ColumnType>(string TableName,
            IEnumerable<IHBaseClassCell<T, ColumnType>> Cells)
            where T : class
        {
            IList<IHBaseClassCell<T, ColumnType>> ReturnCells = new List<IHBaseClassCell<T, ColumnType>>();
            IDictionary<IHBaseCellInfo, object> CellsDictionary = GetCells(TableName, (IEnumerable<IHBaseCellInfo>)Cells);

            foreach (IHBaseClassCell<T, ColumnType> Cell in Cells)
            {
                if ((object)Cell != null)
                {
                    Cell.Value = (T)CellsDictionary[Cell];
                }

                ReturnCells.Add(Cell);
            }

            return ReturnCells;
        }

        public IList<IHBaseStructureCell<T>> GetCells<T>(string TableName, IEnumerable<IHBaseStructureCell<T>> Cells)
            where T : struct
        {
            IList<IHBaseStructureCell<T>> ReturnCells = new List<IHBaseStructureCell<T>>();
            IDictionary<IHBaseCellInfo, object> CellsDictionary = GetCells(TableName, (IEnumerable<IHBaseCellInfo>)Cells);

            foreach (IHBaseStructureCell<T> Cell in Cells)
            {
                if ((object)Cell != null)
                {
                    Cell.Value = (T?)CellsDictionary[Cell];
                }

                ReturnCells.Add(Cell);
            }

            return ReturnCells;
        }

        public IList<IHBaseClassCell<T>> GetCells<T>(string TableName, IEnumerable<IHBaseClassCell<T>> Cells)
            where T : class
        {
            IList<IHBaseClassCell<T>> ReturnCells = new List<IHBaseClassCell<T>>();
            IDictionary<IHBaseCellInfo, object> CellsDictionary = GetCells(TableName, (IEnumerable<IHBaseCellInfo>)Cells);

            foreach (IHBaseClassCell<T> Cell in Cells)
            {
                if ((object)Cell != null)
                {
                    Cell.Value = (T)CellsDictionary[Cell];
                }

                ReturnCells.Add(Cell);
            }

            return ReturnCells;
        }

        public IDictionary<IHBaseCellInfo, object> GetCells(string TableName, IEnumerable<IHBaseCellInfo> Cells)
        {
            Dictionary<string, byte[]> Keys = new Dictionary<string, byte[]>();
            HashSet<byte[]> Columns = new HashSet<byte[]>(new ByteArrayEqualityComparer());

            IList<TRowResult> RowResults;

            foreach (IHBaseCellInfo Cell in Cells)
            {
                if ((object)Cell != null)
                {
                    if (!Keys.ContainsKey(Cell.Key))
                    {
                        Keys.Add(Cell.Key, ClientEncoder.EncodeString(Cell.Key));
                    }

                    byte[] ColumnWithFamily = ClientEncoder.GetColumnFamilyColumnNameByteArray(Cell.ColumnNameType, Cell.ColumnFamily, Cell.Column);

                    if (!Columns.Contains(ColumnWithFamily))
                    {
                        Columns.Add(ColumnWithFamily);
                    }
                }
            }

            RowResults = _ClientPool.Execute(cp =>
            {
                return cp.Execute(c => c.getRowsWithColumns(ClientEncoder.EncodeString(TableName), new List<byte[]>(Keys.Values),
                    new List<byte[]>(Columns), new Dictionary<byte[], byte[]>()));
            });

            IDictionary<string, IDictionary<byte[], byte[]>> RowResultsDictionary = new Dictionary<string, IDictionary<byte[], byte[]>>();

            foreach (TRowResult Result in RowResults)
            {
                string Key = ClientEncoder.DecodeString(Result.Row);

                if (!RowResultsDictionary.ContainsKey(Key))
                {
                    RowResultsDictionary.Add(Key, new Dictionary<byte[], byte[]>(new ByteArrayEqualityComparer()));
                }

                foreach (KeyValuePair<byte[], TCell> kvp in Result.Columns)
                {
                    RowResultsDictionary[Key].Add(kvp.Key, kvp.Value.Value);
                }
            }

            IDictionary<IHBaseCellInfo, object> ReturnDictionary = new Dictionary<IHBaseCellInfo, object>();

            foreach (IHBaseCellInfo Cell in Cells)
            {
                if ((object)Cell != null)
                {
                    byte[] ColumnWithFamily = ClientEncoder.GetColumnFamilyColumnNameByteArray(Cell.ColumnNameType, Cell.ColumnFamily, Cell.Column);

                    if (RowResultsDictionary.ContainsKey(Cell.Key) && RowResultsDictionary[Cell.Key].ContainsKey(ColumnWithFamily))
                    {
                        ReturnDictionary.Add(Cell, ClientReflector.TryGetValueByType(Cell.CellType,
                            RowResultsDictionary[Cell.Key][ColumnWithFamily]));
                    }
                    else
                    {
                        ReturnDictionary.Add(Cell, null);
                    }
                }
            }

            return ReturnDictionary;
        }

        public IDictionary<string, POCO> GetBulk<POCO>(string TableName, IEnumerable<string> Keys, string ColumnFamily)
            where POCO : class
        {
            List<byte[]> Columns = ClientReflector.GetColumns<POCO>(ColumnFamily);
            List<byte[]> EncodedKeys = new List<byte[]>();
            IList<TRowResult> RowResults;

            foreach (string Key in Keys)
            {
                EncodedKeys.Add(ClientEncoder.EncodeString(Key));
            }

            RowResults = _ClientPool.Execute(cp =>
            {
                return cp.Execute(c => c.getRowsWithColumns(ClientEncoder.EncodeString(TableName), EncodedKeys, Columns, new Dictionary<byte[], byte[]>()));
            });

            IDictionary<string, POCO> ProxiesDictionary = new Dictionary<string, POCO>();

            foreach (TRowResult Result in RowResults)
            {
                ProxiesDictionary.Add(ClientEncoder.DecodeString(Result.Row), ClientReflector.GetProxy<POCO>(Result));
            }

            return ProxiesDictionary;
        }
        #endregion

        #region Scan
        public IDictionary<string, POCO> ScanByPrefix<POCO>(string TableName, string ColumnFamily, string KeyPrefix)
            where POCO : class
        {
            IDictionary<string, POCO> ProxiesDictionary = new Dictionary<string, POCO>();
            List<byte[]> Columns = ClientReflector.GetColumns<POCO>(ColumnFamily);

            _ClientPool.Execute(cp =>
            {
                int ScannerID = -1;

                try
                {
                    ScannerID = cp.Execute(c => c.scannerOpenWithPrefix(ClientEncoder.EncodeString(TableName), ClientEncoder.EncodeString(KeyPrefix),
                        Columns, new Dictionary<byte[], byte[]>()));
                    List<TRowResult> RowResults;

                    do
                    {
                        RowResults = cp.Execute(c => c.scannerGetList(ScannerID, DEFAULTNUMSCANROWS));

                        foreach (TRowResult Result in RowResults)
                        {
                            ProxiesDictionary.Add(ClientEncoder.DecodeString(Result.Row), ClientReflector.GetProxy<POCO>(Result));
                        }
                    }
                    while (RowResults.Any());
                }
                finally
                {
                    if (ScannerID >= 0)
                    {
                        cp.Execute(c => c.scannerClose(ScannerID));
                    }
                }
            });

            return ProxiesDictionary;
        }

        public IList<String> GetKeysForPrefix(String TableName, String Prefix)
        {
            IList<String> Keys = new List<String>();
            
            _ClientPool.Execute(cp =>
            {
                int ScannerID = -1;

                try
                {
                    ScannerID = cp.Execute(c => c.scannerOpenWithPrefix(ClientEncoder.EncodeString(TableName), ClientEncoder.EncodeString(Prefix), new List<byte[]>(),
                        new Dictionary<byte[], byte[]>()));

                    List<TRowResult> RowResults;

                    do
                    {
                        RowResults = cp.Execute(c => c.scannerGetList(ScannerID, DEFAULTNUMSCANROWS));

                        foreach (TRowResult Result in RowResults)
                        {
                            Keys.Add(ClientEncoder.DecodeString(Result.Row));
                        }
                    } while (RowResults.Any());
                }
                finally
                {
                    if (ScannerID >= 0)
                    {
                        cp.Execute(c => c.scannerClose(ScannerID));
                    }
                }
            });

            return Keys;
        }

        public IDictionary<string, POCO> Scan<POCO>(string TableName, string ColumnFamily, string KeyStart)
            where POCO : class
        {
            return Scan<POCO>(TableName, ColumnFamily, KeyStart, null);
        }

        public IDictionary<string, POCO> Scan<POCO>(string TableName, string ColumnFamily, string KeyStart, string KeyStop)
            where POCO : class
        {
            IDictionary<string, POCO> ProxiesDictionary = new Dictionary<string, POCO>();
            List<byte[]> Columns = ClientReflector.GetColumns<POCO>(ColumnFamily);

            _ClientPool.Execute(cp =>
            {
                int ScannerID = -1;

                try
                {
                    if (String.IsNullOrEmpty(KeyStop))
                    {
                        ScannerID = cp.Execute(c => c.scannerOpen(ClientEncoder.EncodeString(TableName), ClientEncoder.EncodeString(KeyStart), Columns,
                            new Dictionary<byte[], byte[]>()));
                    }
                    else
                    {
                        ScannerID = cp.Execute(c => c.scannerOpenWithStop(ClientEncoder.EncodeString(TableName), ClientEncoder.EncodeString(KeyStart),
                            ClientEncoder.EncodeString(KeyStop), Columns, new Dictionary<byte[], byte[]>()));
                    }

                    List<TRowResult> RowResults;

                    do
                    {
                        RowResults = cp.Execute(c => c.scannerGetList(ScannerID, DEFAULTNUMSCANROWS));

                        foreach (TRowResult Result in RowResults)
                        {
                            ProxiesDictionary.Add(ClientEncoder.DecodeString(Result.Row), ClientReflector.GetProxy<POCO>(Result));
                        }
                    }
                    while (RowResults.Any());
                }
                finally
                {
                    if (ScannerID >= 0)
                    {
                        cp.Execute(c => c.scannerClose(ScannerID));
                    }
                }
            });

            return ProxiesDictionary;
        }

        public IDictionary<string, POCO> Scan<POCO>(string TableName, string ColumnFamily, IEnumerable<string> SubStrings)
            where POCO : class
        {
            return Scan<POCO>(TableName, ColumnFamily, SubStrings, null, null, null, null, null);
        }

        public IDictionary<string, POCO> Scan<POCO>(string TableName, string ColumnFamily, IEnumerable<string> SubStrings, IEnumerable<string> Prefixes)
            where POCO : class
        {
            return Scan<POCO>(TableName, ColumnFamily, SubStrings, Prefixes, null, null, null, null);
        }

        public IDictionary<string, POCO> Scan<POCO>(string TableName, string ColumnFamily, IEnumerable<string> SubStrings, IEnumerable<string> Prefixes,
            string Regex)
            where POCO : class
        {
            return Scan<POCO>(TableName, ColumnFamily, SubStrings, Prefixes, null, null, Regex, null);
        }

        public IDictionary<string, POCO> Scan<POCO>(string TableName, string ColumnFamily, IEnumerable<string> SubStrings, IEnumerable<string> Prefixes,
            string KeyStart, string KeyStop, string Regex)
            where POCO : class
        {
            return Scan<POCO>(TableName, ColumnFamily, SubStrings, Prefixes, KeyStart, KeyStop, Regex, null);
        }

        public IDictionary<string, POCO> Scan<POCO>(string TableName, string ColumnFamily, IEnumerable<string> SubStrings, IEnumerable<string> Prefixes,
            string KeyStart, string KeyStop, string Regex, int? MaxResults)
            where POCO : class
        {
            IDictionary<string, POCO> ProxiesDictionary = new Dictionary<string, POCO>();
            List<byte[]> Columns = ClientReflector.GetColumns<POCO>(ColumnFamily);

            int ScannerID = -1;
            TScan ScanObj = new TScan();
            ScanObj.Caching = 1000;
            ScanObj.Columns = Columns;

            if ((object)KeyStart != null)
            {
                ScanObj.StartRow = ClientEncoder.EncodeString(KeyStart);
            }

            if ((object)KeyStop != null)
            {
                ScanObj.StopRow = ClientEncoder.EncodeString(KeyStop);
            }

            StringBuilder FilterString = null;

            if ((object)Prefixes != null)
            {
                foreach (string Prefix in Prefixes)
                {
                    if ((object)FilterString == null)
                    {
                        FilterString = new StringBuilder();
                    }
                    else
                    {
                        FilterString.Append(" OR ");
                    }

                    FilterString.Append("PrefixFilter ('");
                    FilterString.Append(Prefix);
                    FilterString.Append("')");
                }
            }

            if ((object)SubStrings != null)
            {
                foreach (string SubString in SubStrings)
                {
                    if ((object)FilterString == null)
                    {
                        FilterString = new StringBuilder();
                    }
                    else
                    {
                        FilterString.Append(" AND ");
                    }

                    FilterString.Append("RowFilter (=, 'substring:");
                    FilterString.Append(SubString);
                    FilterString.Append("')");
                }
            }

            if ((object)Regex != null)
            {
                if ((object)FilterString == null)
                {
                    FilterString = new StringBuilder();
                }
                else
                {
                    FilterString.Append(" AND ");
                }
                FilterString.Append("RowFilter (=, 'regexstring:");
                FilterString.Append(Regex);
                FilterString.Append("')");
            }

            ScanObj.FilterString = ClientEncoder.EncodeString(FilterString.ToString());

            _ClientPool.Execute(cp =>
            {
                try
                {
                    ScannerID = cp.Execute(c => c.scannerOpenWithScan(ClientEncoder.EncodeString(TableName), ScanObj, new Dictionary<byte[], byte[]>()));
                    IList<TRowResult> RowResults;

                    if (!MaxResults.HasValue) MaxResults = Int32.MaxValue;

                    do
                    {
                        RowResults = cp.Execute(c => c.scannerGetList(ScannerID, Math.Min(DEFAULTNUMSCANROWS, MaxResults.Value - ProxiesDictionary.Count)));

                        foreach (TRowResult Result in RowResults)
                        {
                            ProxiesDictionary.Add(ClientEncoder.DecodeString(Result.Row), ClientReflector.GetProxy<POCO>(Result));
                        }

                        if (ProxiesDictionary.Count >= MaxResults) break;
                    }
                    while (RowResults.Any());
                }
                finally
                {
                    if (ScannerID >= 0)
                    {
                        cp.Execute(c => c.scannerClose(ScannerID));
                    }
                }
            });

            return ProxiesDictionary;
        }

        public IDictionary<string, POCO> ScanByRegex<POCO>(string TableName, string ColumnFamily, string Regex)
            where POCO : class
        {
            return Scan<POCO>(TableName, ColumnFamily, null, null, null, null, Regex);
        }

        public IDictionary<string, POCO> ScanByRegex<POCO>(string TableName, string ColumnFamily, string KeyStart, string Regex)
            where POCO : class
        {
            return Scan<POCO>(TableName, ColumnFamily, null, null, KeyStart, null, Regex);
        }

        public IDictionary<string, POCO> ScanByRegex<POCO>(string TableName, string ColumnFamily, string KeyStart, string KeyStop, string Regex)
            where POCO : class
        {
            return Scan<POCO>(TableName, ColumnFamily, null, null, KeyStart, KeyStop, Regex);
        }

        public IDictionary<string, POCO> ScanByPrefixesWithRegex<POCO>(string TableName, string ColumnFamily, IEnumerable<string> Prefixes, string Regex)
            where POCO : class
        {
            return Scan<POCO>(TableName, ColumnFamily, null, Prefixes, null, null, Regex);
        }
        #endregion

        #region Atomic
        public long AtomicIncrement(string TableName, string Key, string ColumnFamily, string Column, long Value)
        {
            return _ClientPool.Execute(cp =>
            {
                return cp.Execute(c => c.atomicIncrement(ClientEncoder.EncodeString(TableName), ClientEncoder.EncodeString(Key),
                    ClientEncoder.GetColumnFamilyColumnNameByteArray(ColumnFamily, Column), Value));
            });
        }

        public long AtomicIncrement(string TableName, string Key, string ColumnFamily, long Value, DateTime TimeStamp, TimeInterval Interval)
        {
            return _ClientPool.Execute(cp =>
            {
                return cp.Execute(c => c.atomicIncrement(ClientEncoder.EncodeString(TableName), ClientEncoder.EncodeString(Key),
                    ClientEncoder.GetColumnFamilyColumnNameByteArray(ColumnFamily, TimeStamp, Interval), Value));
            });
        }

        public void BulkAtomicIncrement(IEnumerable<IAtomicIncrement> Increments)
        {
            List<TIncrement> TIncrements = new List<TIncrement>();

            foreach (IAtomicIncrement Increment in Increments)
            {
                TIncrements.Add(new TIncrement()
                {
                    Ammount = Increment.Value,
                    Column = ClientEncoder.GetColumnFamilyColumnNameByteArray(Increment.ColumnFamily, Increment.Column),
                    Row = ClientEncoder.EncodeString(Increment.Key),
                    Table = ClientEncoder.EncodeString(Increment.TableName)
                });
            }

            _ClientPool.Execute(cp =>
            {
                cp.Execute(c => c.incrementRows(TIncrements));
            });
        }
        #endregion

        #region Table Functions
        public void CreateTable(string TableName, IEnumerable<string> ColumnFamilies, CompressionType Compression)
        {
            List<ColumnDescriptor> Descriptors = new List<ColumnDescriptor>();

            foreach (string Family in ColumnFamilies)
            {
                ColumnDescriptor Descriptor = new ColumnDescriptor();

                Descriptor.Name = ClientEncoder.EncodeString(Family);

                if (Compression == CompressionType.Snappy)
                {
                    Descriptor.Compression = "SNAPPY";
                }

                Descriptors.Add(Descriptor);
            }

            _ClientPool.Execute(cp =>
            {
                cp.Execute(c => c.createTable(ClientEncoder.EncodeString(TableName), Descriptors));
            });
        }

        public void DeleteTable(string TableName)
        {
            _ClientPool.Execute(cp =>
            {
                cp.Execute(c => c.disableTable(ClientEncoder.EncodeString(TableName)));
            });

            _ClientPool.Execute(cp =>
            {
                cp.Execute(c => c.deleteTable(ClientEncoder.EncodeString(TableName)));
            });
        }

        public IList<string> GetTableNames()
        {
            List<Byte[]> EncodedTables;

            EncodedTables = _ClientPool.Execute(cp =>
            {
                return cp.Execute(c => c.getTableNames());
            });

            IList<string> Tables = new List<string>();

            foreach (byte[] Table in EncodedTables)
            {
                Tables.Add(ClientEncoder.DecodeString(Table));
            }

            return Tables;
        }
        #endregion

        #region IDisposable Support
        private bool Disposed;

        protected virtual void Dispose(bool Disposing)
        {
            if (!Disposed && Disposing)
            {
                if ((object)_ClientPool != null)
                {
                    _ClientPool.Dispose();
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

    public interface IClient
        : IDisposable
    {
        int AvailableClients { get; }
        int BlockedRequests { get; }
        int TotalClients { get; }

        long AtomicIncrement(string TableName, string Key, string ColumnFamily, long Value, DateTime TimeStamp, TimeInterval Interval);
        long AtomicIncrement(string TableName, string Key, string ColumnFamily, string Column, long Value);

        /// <summary>
        /// NOTE: Bulk atomic requires HBase version .94 or higher!
        /// </summary>
        /// <typeparam name="POCO"></typeparam>
        /// <param name="Increments"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        void BulkAtomicIncrement(IEnumerable<IAtomicIncrement> Increments);
        void CreateTable(string TableName, IEnumerable<string> ColumnFamilies, CompressionType Compression);
        void DeleteBulk(string TableName, IEnumerable<IHBaseDeletion> Bulk);
        void DeleteOne<POCO>(string TableName, string Key, string ColumnFamily)
            where POCO : class;
        void DeleteRow(string TableName, string Key);
        void DeleteTable(string TableName);
        IDictionary<string, POCO> GetBulk<POCO>(string TableName, IEnumerable<string> Keys, string ColumnFamily)
            where POCO : class;
        IDictionary<IHBaseCellInfo, object> GetCells(string TableName, IEnumerable<IHBaseCellInfo> Cells);
        IList<IHBaseClassCell<T, ColumnType>> GetCells<T, ColumnType>(string TableName, IEnumerable<IHBaseClassCell<T, ColumnType>> Cells)
            where T : class;
        IList<IHBaseStructureCell<T, ColumnType>> GetCells<T, ColumnType>(string TableName, IEnumerable<IHBaseStructureCell<T, ColumnType>> Cells)
            where T : struct;
        IList<IHBaseClassCell<T>> GetCells<T>(string TableName, IEnumerable<IHBaseClassCell<T>> Cells)
            where T : class;
        IList<IHBaseStructureCell<T>> GetCells<T>(string TableName, IEnumerable<IHBaseStructureCell<T>> Cells)
            where T : struct;
        T GetClassCell<T, ColumnNameType>(string TableName, string Key, string ColumnFamily, ColumnNameType Column)
            where T : class;
        T GetClassCell<T>(string TableName, string Key, string ColumnFamily, string Column)
            where T : class;
        POCO GetOne<POCO>(string TableName, string Key, DateTime TimeStamp, TimeInterval Interval)
            where POCO : class;
        POCO GetOne<POCO>(string TableName, string Key, string ColumnFamily)
            where POCO : class;
        IDictionary<string, long> GetOptimizationCounts(string TableName, IEnumerable<IHBaseIndexOptimizationLookup> IndexOptimizationLookups);
        IDictionary<long, string> GetRange(string TableName, string Key, string Index, long Start, long Stop);
        IDictionary<string, IDictionary<DateTime, POCO>> GetRange<POCO>(string TableName, IEnumerable<string> Keys, DateTime StartTime,
            DateTime StopTime, TimeInterval Interval)
            where POCO : class;
        IDictionary<DateTime, POCO> GetRange<POCO>(string TableName, string Key, DateTime StartTime, DateTime StopTime, TimeInterval Interval)
            where POCO : class;
        IList<IHBaseCell> GetRow(string TableName, string Key, IDictionary<string, string> Attributes);
        IList<IHBaseCell<ReturnType>> GetRow<ReturnType>(string TableName, string Key, IDictionary<string, string> Attributes);
        IList<IHBaseCell<ReturnType, ColumnType>> GetRow<ReturnType, ColumnType>(string TableName, string Key,
            IDictionary<string, string> Attributes);
        T? GetStructureCell<T, ColumnNameType>(string TableName, string Key, string ColumnFamily, ColumnNameType Column)
            where T : struct;
        T? GetStructureCell<T>(string TableName, string Key, string ColumnFamily, string Column)
            where T : struct;
        IList<string> GetTableNames();
        void PutBulk(string TableName, IEnumerable<IHBasePut> Bulk);
        void PutOne<POCO>(string TableName, string Key, string ColumnFamily, POCO Value)
            where POCO : class;
        IDictionary<string, POCO> Scan<POCO>(string TableName, string ColumnFamily, IEnumerable<string> SubStrings)
            where POCO : class;
        IDictionary<string, POCO> Scan<POCO>(string TableName, string ColumnFamily, IEnumerable<string> SubStrings, IEnumerable<string> Prefixes)
            where POCO : class;
        IDictionary<string, POCO> Scan<POCO>(string TableName, string ColumnFamily, IEnumerable<string> SubStrings, IEnumerable<string> Prefixes, string KeyStart, string KeyStop,
            string Regex)
            where POCO : class;
        IDictionary<string, POCO> Scan<POCO>(string TableName, string ColumnFamily, IEnumerable<string> SubStrings, IEnumerable<string> Prefixes, string KeyStart, string KeyStop,
            string Regex, int? MaxResults)
            where POCO : class;
        IDictionary<string, POCO> Scan<POCO>(string TableName, string ColumnFamily, IEnumerable<string> SubStrings, IEnumerable<string> Prefixes, string Regex)
            where POCO : class;
        IDictionary<string, POCO> Scan<POCO>(string TableName, string ColumnFamily, string KeyStart)
            where POCO : class;
        IDictionary<string, POCO> Scan<POCO>(string TableName, string ColumnFamily, string KeyStart, string KeyStop)
            where POCO : class;
        IList<String> GetKeysForPrefix(String TableName, String Prefix);
        IDictionary<string, POCO> ScanByPrefix<POCO>(string TableName, string ColumnFamily, string KeyPrefix)
            where POCO : class;
        IDictionary<string, POCO> ScanByPrefixesWithRegex<POCO>(string TableName, string ColumnFamily, IEnumerable<string> Prefixes, string Regex)
            where POCO : class;
        IDictionary<string, POCO> ScanByRegex<POCO>(string TableName, string ColumnFamily, string KeyStart, string KeyStop, string Regex)
            where POCO : class;
        IDictionary<string, POCO> ScanByRegex<POCO>(string TableName, string ColumnFamily, string KeyStart, string Regex)
            where POCO : class;
        IDictionary<string, POCO> ScanByRegex<POCO>(string TableName, string ColumnFamily, string Regex)
            where POCO : class;
    }
}
