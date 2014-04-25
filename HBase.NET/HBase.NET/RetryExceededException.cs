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
using System.Runtime.Serialization;

namespace Hbase
{
    [Serializable]
    public class RetryExceededException
        : ApplicationException,
        ISerializable
    {
        public int NumberOfRetries { get; private set; }

        public RetryExceededException(int numberOfRetires, Exception innerException)
            : this(numberOfRetires, String.Format("The Operation has been retried {0} times. Last error was {1}: {2}.", numberOfRetires, innerException.GetType().Name, innerException.Message), innerException)
        { }

        public RetryExceededException(int numberOfRetries, string message, Exception innerException)
            : base(message, innerException)
        {
            this.NumberOfRetries = numberOfRetries;
        }

        public RetryExceededException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {

            NumberOfRetries = info.GetInt32("NumberOfRetries");
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("NumberOfRetries", NumberOfRetries);
        }
    }
}
