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

namespace Hbase
{
    public class HBaseClassCell<CellType>
        : HBaseClassCell<CellType, string>
        where CellType : class
    {
        public HBaseClassCell(string Key, string ColumnFamily, string Column)
            : base(Key, ColumnFamily, Column) { }
    }

    public class HBaseClassCell<CellType, ColumnNameType>
        : HBaseCellInfo<ColumnNameType>,
        IHBaseClassCell<CellType, ColumnNameType>
        where CellType : class
    {
        private CellType _Value = null;

        public HBaseClassCell(string Key, string ColumnFamily, ColumnNameType Column)
        {
            _Key = Key;
            _ColumnFamily = ColumnFamily;
            _Column = Column;
            _CellType = typeof(CellType);
            _ColumnNameType = typeof(ColumnNameType);
        }

        public CellType Value
        {
            get
            {
                return _Value;
            }

            set
            {
                _Value = value;
            }
        }
    }

    public interface IHBaseClassCell<CellType, ColumnNameType>
        : IHBaseCellInfo<ColumnNameType>,
        IHBaseClassCell<CellType>               
        where CellType : class { }

    public interface IHBaseClassCell<CellType>
        : IHBaseCellInfo
        where CellType : class
    {
        CellType Value
        {
            get;
            set;
        }
    }
}