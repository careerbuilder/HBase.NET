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

namespace Hbase.Iterators.RoundRobin
{
    public class RoundRobinIterator<T> : IIterator<T>
    {
        private int _NextElement = 0;
        private List<T> _Elements = new List<T>();

        public RoundRobinIterator(IEnumerable<T> elements)
        {
            this._Elements.AddRange(elements);
        }

        public T Next()
        {
            try
            {
                return this._Elements[this._NextElement];
            }
            finally
            {
                this._NextElement = (this._NextElement + 1) % this._Elements.Count;
            }
        }
    }
}
