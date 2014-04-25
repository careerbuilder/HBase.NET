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
    public abstract class HBaseMutation<POCO>
        : HBaseMutation,
        IHBaseMutation<POCO>
        where POCO : class
    {
        public new POCO Value
        {
            get
            {
                return (POCO)_Value;
            }
        }

        public abstract override bool IsDelete
        {
            get;
        }
    }

    public abstract class HBaseMutation
        : IHBaseMutation
    {
        protected string _Key = null;
        protected string _ColumnFamily = null;
        protected object _Value = null;
        protected Type _ValueType = null;

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

        public object Value
        {
            get
            {
                return _Value;
            }
        }

        public Type ValueType
        {
            get
            {
                return _ValueType;
            }
        }

        public abstract bool IsDelete
        {
            get;
        }
    }

    public interface IHBaseMutation<POCO>
        : IHBaseMutation
    {
        new POCO Value
        {
            get;
        }
    }

    public interface IHBaseMutation
    {
        string Key
        {
            get;
        }

        string ColumnFamily
        {
            get;
        }

        object Value
        {
            get;
        }

        Type ValueType
        {
            get;
        }

        bool IsDelete
        {
            get;
        }
    }
}