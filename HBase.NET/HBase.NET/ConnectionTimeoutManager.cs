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
    internal class ConnectionTimeoutManager
    {
        public static int GetRemainingTimeout(int Timeout, DateTime TimeStamp)
        {
            return GetRemainingTimeout(Timeout, TimeStamp, null);
        }

        public static int GetRemainingTimeout(int Timeout, DateTime TimeStamp, Exception InnerEx)
        {
            int ReturnTimeout = Timeout - (int)(DateTime.Now - TimeStamp).TotalMilliseconds;

            if (ReturnTimeout <= 0)
            {
                if ((object)InnerEx == null)
                {
                    throw new TimeoutException("The operation has timed out.");
                }
                else
                {
                    throw new TimeoutException("The operation has timed out.", InnerEx);
                }
            }

            return ReturnTimeout;
        }
    }
}
