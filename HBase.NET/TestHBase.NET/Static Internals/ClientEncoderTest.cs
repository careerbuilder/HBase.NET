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
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Hbase;
using Hbase.StaticInternals;
using System.Reflection;
using System.IO;

namespace TestHBase.NET.Static_Internals
{
    [TestClass]
    public class ClientEncoderTest
    {
        [TestMethod]
        public void GetMutationValueByTypeCode_ReturnsAppropriateByteArrays()
        {
            DateTime Birthday = DateTime.Parse("07/24/1987 1:12:05 PM");

            CollectionAssert.AreEqual(new byte[] { 0 }, GetMutationValueByTypeCode(false));
            CollectionAssert.AreEqual(new byte[] { 1 }, GetMutationValueByTypeCode(true));
            CollectionAssert.AreEqual(new byte[] { 0x4C }, GetMutationValueByTypeCode((byte)0x4C));
            CollectionAssert.AreEqual(ClientEncoder.EncodeString("Q"), GetMutationValueByTypeCode('Q'));
            CollectionAssert.AreEqual(ClientEncoder.EncodeString(Birthday.ToString(ClientEncoder.DATEFORMAT) + ".0"), GetMutationValueByTypeCode(Birthday));
            CollectionAssert.AreEqual(ClientEncoder.EncodeString(3.14159m.ToString()), GetMutationValueByTypeCode(3.14159m));
            CollectionAssert.AreEqual(Reverse(BitConverter.GetBytes(93484932.234)), GetMutationValueByTypeCode(93484932.234));
            CollectionAssert.AreEqual(Reverse(BitConverter.GetBytes((short)124)), GetMutationValueByTypeCode((short)124));
            CollectionAssert.AreEqual(Reverse(BitConverter.GetBytes(12346)), GetMutationValueByTypeCode(12346));
            CollectionAssert.AreEqual(Reverse(BitConverter.GetBytes(12345678902L)), GetMutationValueByTypeCode(12345678902L));

            unchecked
            {
                CollectionAssert.AreEqual(new byte[] { (byte)0xFF }, GetMutationValueByTypeCode((sbyte)(0xFF)));
            }

            CollectionAssert.AreEqual(Reverse(BitConverter.GetBytes(1.4f)), GetMutationValueByTypeCode(1.4f));
            CollectionAssert.AreEqual(ClientEncoder.EncodeString("foo"), GetMutationValueByTypeCode("foo"));
            CollectionAssert.AreEqual(Reverse(BitConverter.GetBytes((ushort)125)), GetMutationValueByTypeCode((ushort)125));
            CollectionAssert.AreEqual(Reverse(BitConverter.GetBytes(12347U)), GetMutationValueByTypeCode(12347U));
            CollectionAssert.AreEqual(Reverse(BitConverter.GetBytes(12345678903UL)), GetMutationValueByTypeCode(12345678903UL));
        }

        [TestMethod]
        public void TryGetValueForStructureByTypeCode_ReturnsAppropriateT()
        {
            DateTime Birthday = DateTime.Parse("07/24/1987 1:12:05 PM");

            Assert.AreEqual(false, TryGetValueForStructureByTypeCode<bool>(new byte[] { 0 }));
            Assert.AreEqual(true, TryGetValueForStructureByTypeCode<bool>(new byte[] { 1 }));
            Assert.AreEqual((byte)0x4C, TryGetValueForStructureByTypeCode<byte>(new byte[] { 0x4C }));
            Assert.AreEqual('Q', TryGetValueForStructureByTypeCode<char>(ClientEncoder.EncodeString("Q")));
            Assert.AreEqual(Birthday, TryGetValueForStructureByTypeCode<DateTime>(ClientEncoder.EncodeString(Birthday.ToString(ClientEncoder.DATEFORMAT) + ".0")));
            Assert.AreEqual(3.14159m, TryGetValueForStructureByTypeCode<decimal>(ClientEncoder.EncodeString(3.14159m.ToString())));
            Assert.AreEqual(93484932.234, TryGetValueForStructureByTypeCode<double>(Reverse(BitConverter.GetBytes(93484932.234))));
            Assert.AreEqual((short)124, TryGetValueForStructureByTypeCode<short>(Reverse(BitConverter.GetBytes((short)124))));
            Assert.AreEqual(12346, TryGetValueForStructureByTypeCode<int>(Reverse(BitConverter.GetBytes(12346))));
            Assert.AreEqual(12345678902L, TryGetValueForStructureByTypeCode<long>(Reverse(BitConverter.GetBytes(12345678902L))));

            unchecked
            {
                Assert.AreEqual((sbyte)0xFF, TryGetValueForStructureByTypeCode<sbyte>(new byte[] { (byte)0xFF }));
            }

            Assert.AreEqual(1.4f, TryGetValueForStructureByTypeCode<float>(Reverse(BitConverter.GetBytes(1.4f))));
            Assert.AreEqual((ushort)125, TryGetValueForStructureByTypeCode<ushort>(Reverse(BitConverter.GetBytes((ushort)125))));
            Assert.AreEqual(12347U, TryGetValueForStructureByTypeCode<uint>(Reverse(BitConverter.GetBytes(12347U))));
            Assert.AreEqual(12345678903UL, TryGetValueForStructureByTypeCode<ulong>(Reverse(BitConverter.GetBytes(12345678903UL))));
        }

        [TestMethod]
        public void TryGetValueForObjectByTypeCode_ReturnsAppropriateStrings()
        {
            Assert.AreEqual("foo", TryGetValueForObjectByTypeCode<string>(ClientEncoder.EncodeString("foo")));
            Assert.IsNull(TryGetValueForObjectByTypeCode<string>(new byte[] { }));
            Assert.IsNull(TryGetValueForObjectByTypeCode<string>(null));
        }

        private static byte[] GetMutationValueByTypeCode(object Input)
        {
            return ClientEncoder.GetBytesByType(Input.GetType(), Input);
        }

        private static byte[] Reverse(byte[] Input)
        {
            if ((object)Input != null)
            {
                Array.Reverse(Input);
            }

            return Input;
        }

        private static T TryGetValueForStructureByTypeCode<T>(byte[] Value)
            where T : struct
        {
            MethodInfo Method = typeof(ClientEncoder).GetMethod("TryGetValueForStructureByTypeCode",
                BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.InvokeMethod);

            MethodInfo SpecificTypeMethod = Method.MakeGenericMethod(typeof(T));

            return (T)SpecificTypeMethod.Invoke(null, new object[] { Value });
        }

        private static T TryGetValueForObjectByTypeCode<T>(byte[] Value)
            where T : class
        {
            MethodInfo Method = typeof(ClientEncoder).GetMethod("TryGetValueForObjectByTypeCode",
                BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.InvokeMethod);

            MethodInfo SpecificTypeMethod = Method.MakeGenericMethod(typeof(T));

            return (T)SpecificTypeMethod.Invoke(null, new object[] { Value });
        }
    }
}
