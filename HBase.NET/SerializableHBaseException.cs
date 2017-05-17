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
using System.Collections;
using System.Reflection;

namespace Hbase
{
    [Serializable]
    public class SerializableHBaseException
        : Exception,
        ISerializable
    {
        private const BindingFlags FIELDATTRIBUTES = BindingFlags.GetField | BindingFlags.Instance | BindingFlags.NonPublic;
        private const BindingFlags INVOKEATTRIBUTES = BindingFlags.Instance | BindingFlags.InvokeMethod | BindingFlags.NonPublic;
        private const BindingFlags STATICINVOKEATTRIBUTES = BindingFlags.Static | BindingFlags.InvokeMethod | BindingFlags.NonPublic;

        public SerializableHBaseException(SerializationInfo info, StreamingContext context)
            : base(info, context) { }

        public SerializableHBaseException(string message, Exception innerException)
            : base(message, innerException) { }

        //create a fake info object which has the parameters of the inner exception so on deserialization, it retains a majority
        //of the attributes of the old exception. If the innerex does not exist, then it has already been serialized once and it
        //is safe to use the data on this exception, as we have already propagated the attributes.
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if ((object)this.InnerException != null)
            {
                string stackTraceString = (string)typeof(Exception).GetField("_stackTraceString", FIELDATTRIBUTES).GetValue(this.InnerException);
                if (typeof(Exception).GetField("_stackTrace", FIELDATTRIBUTES).GetValue(this.InnerException) != null)
                {
                    if (stackTraceString == null)
                        stackTraceString = (string)typeof(Environment).GetMethod("GetStackTrace", STATICINVOKEATTRIBUTES).Invoke(null,
                            new object[] {this.InnerException, true});
                    if (typeof(Exception).GetField("_exceptionMethod", FIELDATTRIBUTES).GetValue(this.InnerException) == null)
                        typeof(Exception).GetField("_exceptionMethod", FIELDATTRIBUTES).SetValue(this.InnerException,
                            typeof(Exception).GetMethod("GetExceptionMethodFromStackTrace", INVOKEATTRIBUTES).Invoke(this.InnerException, null));
                }

                info.AddValue("ClassName", this.InnerException.GetType().ToString(), typeof(string));
                info.AddValue("Message", this.InnerException.Message, typeof(string));
                info.AddValue("Data", this.InnerException.Data, typeof(IDictionary));
                info.AddValue("HelpURL", this.InnerException.HelpLink, typeof(string));
                info.AddValue("StackTraceString", stackTraceString);
                info.AddValue("RemoteStackTraceString", typeof(Exception).GetField("_remoteStackTraceString", FIELDATTRIBUTES).GetValue(this.InnerException),
                    typeof(string));
                info.AddValue("RemoteStackIndex", typeof(Exception).GetField("_remoteStackIndex", FIELDATTRIBUTES).GetValue(this.InnerException),
                    typeof(int));
                info.AddValue("HResult", InnerException.GetType().GetField("_HResult", FIELDATTRIBUTES).GetValue(this.InnerException));
                info.AddValue("ExceptionMethod", typeof(Exception).GetMethod("GetExceptionMethodString", INVOKEATTRIBUTES).Invoke(this.InnerException, null));
                info.AddValue("Source", (object)this.InnerException.Source, typeof(string));
                info.AddValue("WatsonBuckets", typeof(Exception).GetField("_watsonBuckets", FIELDATTRIBUTES).GetValue(this.InnerException), typeof(byte[]));

                //TODO: Make the inner exception more robust.
                info.AddValue("InnerException", null, typeof(Exception));
            }
            else
            {
                base.GetObjectData(info, context);
            }
        }
    }
}
