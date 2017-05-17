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
using Hbase.StaticInternals;

namespace Hbase
{
    public class HBaseCell<ReturnType, ColumnType>
        : HBaseCellBase,
        IHBaseCell<ReturnType, ColumnType>
    {
        private byte[] _Value;

        internal HBaseCell(string ColumnFamily, byte[] Column, byte[] Value)
        {
            _ColumnFamily = ColumnFamily;
            _Column = Column;
            _Value = Value;
        }

        public new ColumnType Column()
        {
            return (ColumnType)ClientReflector.TryGetValueByType(typeof(ColumnType), (byte[])_Column);
        }

        public ReturnType Value()
        {
            return (ReturnType)ClientReflector.TryGetValueByType(typeof(ReturnType), _Value);
        }
    }

    public class HBaseCell<ReturnType>
        : HBaseCellBase,
        IHBaseCell<ReturnType>
    {
        private byte[] _Value;

        internal HBaseCell(string ColumnFamily, byte[] Column, byte[] Value)
        {
            _ColumnFamily = ColumnFamily;
            _Column = Column;
            _Value = Value;
        }

        public new string Column()
        {
            return Column<string>();
        }

        public new T Column<T>()
        {
            return (T)ClientReflector.TryGetValueByType(typeof(T), (byte[])_Column);
        }
        
        public ReturnType Value()
        {
            return (ReturnType)ClientReflector.TryGetValueByType(typeof(ReturnType), _Value);
        }
    }

    public class HBaseCell
        : HBaseCellBase,
        IHBaseCell
    {
        private byte[] _Value;

        internal HBaseCell(string ColumnFamily, byte[] Column, byte[] Value)
        {
            _ColumnFamily = ColumnFamily;
            _Column = Column;
            _Value = Value;
        }

        public new string Column()
        {
            return Column<string>();
        }

        public new T Column<T>()
        {
            return (T)ClientReflector.TryGetValueByType(typeof(T), (byte[])_Column);
        }

        public string Value()
        {
            return Value<string>();
        }

        public T Value<T>()
        {
            return (T)ClientReflector.TryGetValueByType(typeof(T), _Value);
        }
    }

    public abstract class HBaseCellBase
        : IHBaseCellBase
    {
        protected string _ColumnFamily = null;
        protected object _Column = null;

        public string ColumnFamily
        {
            get
            {
                return _ColumnFamily;
            }
        }

        public object Column
        {
            get
            {
                return _Column;
            }
        }
    }

    public interface IHBaseCell<ReturnType, ColumnType>
        : IHBaseCellStaticColumn<ColumnType>,
        IHBaseCellStaticReturn<ReturnType> { }

    public interface IHBaseCell<ReturnType>
        : IHBaseCellStaticReturn<ReturnType>,
        IHBaseCellGenericColumn { }

    public interface IHBaseCell
        : IHBaseCellGenericReturn,
        IHBaseCellGenericColumn { }

    public interface IHBaseCellStaticReturn<ReturnType>
        : IHBaseCellBase
    {
        ReturnType Value();
    }

    public interface IHBaseCellStaticColumn<ColumnType>
        : IHBaseCellBase
    {
        new ColumnType Column();
    }

    public interface IHBaseCellGenericReturn
        : IHBaseCellBase
    {
        string Value();
        T Value<T>();
    }

    public interface IHBaseCellGenericColumn
        : IHBaseCellBase
    {
        new string Column();
        new T Column<T>();
    }

    public interface IHBaseCellBase<ColumnNameType>
        : IHBaseCellBase
    {
        new ColumnNameType Column
        {
            get;
        }
    }

    public interface IHBaseCellBase
    {
        string ColumnFamily
        {
            get;
        }

        object Column
        {
            get;
        }
    }
}