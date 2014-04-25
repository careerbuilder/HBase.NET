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
using Castle.DynamicProxy;
using System.Collections;

namespace Hbase
{
    internal class PropertyInterceptor
        : IInterceptor
    {
        private Hashtable _Internals = new Hashtable();

        public void Intercept(IInvocation invocation)
        {
            string Method = invocation.Method.Name;

            if (Method.Length > 4)
            {
                switch( Method.Substring(0,4))
                {
                    case "get_":
                        invocation.ReturnValue = _Internals[GetMethodName(Method)];

                        break;
                    case "set_":
                        _Internals[GetMethodName(Method)] = invocation.Arguments.FirstOrDefault();

                        break;
                }
            }
        }

        private static string GetMethodName(string Method)
        {
            return Method.Substring(4, Method.Length - 4);
        }
    }
}