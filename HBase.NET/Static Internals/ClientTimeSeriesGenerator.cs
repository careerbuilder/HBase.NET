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

namespace Hbase.StaticInternals
{
    internal class ClientTimeSeriesGenerator
    {
        private const string HOURLYDATEFORMAT = "yyyyMMddHH";

        internal static string GetTimeSeriesColumn(DateTime TimeStamp, TimeInterval Interval)
        {
            string ReturnString = null;

            switch (Interval)
            {
                case TimeInterval.Hourly:
                    ReturnString = TimeStamp.ToString(HOURLYDATEFORMAT);
                    break;
                default:
                    throw new ArgumentException("The TimeInterval is invalid or unsupported");
            }

            return ReturnString;
        }

        internal static IDictionary<DateTime, IDictionary<byte[], TCell>> SegmentTimeSeriesColumns(IDictionary<byte[], TCell> Columns, TimeInterval Interval)
        {
            IDictionary<DateTime, IDictionary<byte[], TCell>> SegmentedColumns = new Dictionary<DateTime, IDictionary<byte[], TCell>>();

            foreach (KeyValuePair<byte[], TCell> kvp in Columns)
            {
                DateTime TimeStamp;

                switch (Interval)
                {
                    case TimeInterval.Hourly:
                        TimeStamp = ClientEncoder.TryParseDate(ClientEncoder.GetColumnName(kvp.Key), HOURLYDATEFORMAT).Value;
                    break;
                    default:
                        throw new ArgumentException("The TimeInterval is invalid or unsupported");
                }

                if (!SegmentedColumns.ContainsKey(TimeStamp))
                {
                    SegmentedColumns.Add(TimeStamp, new Dictionary<byte[], TCell>());
                }

                SegmentedColumns[TimeStamp].Add(kvp);
            }

            return SegmentedColumns;
        }
    }
}
