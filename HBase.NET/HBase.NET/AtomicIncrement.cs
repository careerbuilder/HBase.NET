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
    public class AtomicIncrement
        : IAtomicIncrement
    {
        private readonly string _TableName;
        private readonly string _Key;
        private readonly string _ColumnFamily;
        private readonly string _Column;
        private readonly long _Value;

        public AtomicIncrement(string TableName, string Key, string ColumnFamily, string Column, long Value)
        {
            _TableName = TableName;
            _Key = Key;
            _ColumnFamily = ColumnFamily;
            _Column = Column;
            _Value = Value;
        }

        public string TableName
        {
            get
            {
                return _TableName;
            }
        }

        public string Key
        {
            get
            {
                return _Key;
            }
        }

        public string ColumnFamily
        {
            get
            {
                return _ColumnFamily;
            }
        }

        public string Column
        {
            get
            {
                return _Column;
            }
        }

        public long Value
        {
            get
            {
                return _Value;
            }
        }
    }

    public interface IAtomicIncrement
    {
        string TableName
        {
            get;
        }

        string Key
        {
            get;
        }
        
        string ColumnFamily
        {
            get;
        }
        
        string Column
        {
            get;
        }

        long Value
        {
            get;
        }
    }
}