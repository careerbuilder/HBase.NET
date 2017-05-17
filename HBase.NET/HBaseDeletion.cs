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
    public class HBaseDeletion<POCO>
        : HBaseMutation<POCO>,
        IHBaseDeletion
        where POCO : class
    {
        public HBaseDeletion(string Key, string ColumnFamily)
        {
            _Key = Key;
            _ColumnFamily = ColumnFamily;
            _ValueType = typeof(POCO);
        }

        public override bool IsDelete
        {
            get
            {
                return true;
            }
        }
    }

    public interface IHBaseDeletion
        : IHBaseMutation { }
}