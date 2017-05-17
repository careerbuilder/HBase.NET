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
    public abstract class HBaseCellInfo<ColumnNameType>
        : HBaseCellInfo,
        IHBaseCellInfo<ColumnNameType>
    {
        public new ColumnNameType Column
        {
            get
            {
                return (ColumnNameType)_Column;
            }
        }
    }

    public abstract class HBaseCellInfo
        : HBaseCellBase,
        IHBaseCellInfo
    {
        protected string _Key = null;
        protected Type _CellType = null;
        protected Type _ColumnNameType = null;
        
        public string Key
        {
            get
            {
                return _Key;
            }
        }

        public Type CellType
        {
            get
            {
                return _CellType;
            }
        }

        public Type ColumnNameType
        {
            get
            {
                return _ColumnNameType;
            }
        }
    }

    public interface IHBaseCellInfo<ColumnNameType>
        : IHBaseCellBase<ColumnNameType>,
        IHBaseCellInfo { }
    
    public interface IHBaseCellInfo
        : IHBaseCellBase
    {
        string Key
        {
            get;
        }
        
        Type CellType
        {
            get;
        }

        Type ColumnNameType
        {
            get;
        }
    }
}