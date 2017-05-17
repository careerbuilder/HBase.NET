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
    internal class ByteArrayEqualityComparer
        : IEqualityComparer<byte[]>
    {
        public bool Equals(byte[] a1, byte[] a2)
        {
            bool ReturnValue = false;

            if ((object)a1 == null || (object)a2 == null)
            {
                ReturnValue = (object)a1 == null && (object)a2 == null;
            }
            else
            {
                if (a1.Length == a2.Length)
                {
                    bool DoArraysMatch = true;

                    for (int i = 0; i < a1.Length; ++i)
                    {
                        if (a1[i] != a2[i])
                        {
                            DoArraysMatch = false;
                            break;
                        }
                    }

                    ReturnValue = DoArraysMatch;
                }
            }

            return ReturnValue;
        }

        public int GetHashCode(byte[] a)
        {
            int HashCode = 0;

            for (int i = 0; i < a.Length; i += 4)
            {
                byte[] FourByteSubset = new byte[4];

                for(int j = 0; j < 4; ++j)
                {
                    int loc = (i * 4) + j;

                    if (loc < a.Length)
                    {
                        FourByteSubset[j] = a[loc];
                    }
                    else
                    {
                        FourByteSubset[j] = 0;
                    }
                }

                HashCode ^= BitConverter.ToInt32(FourByteSubset, 0);
            }

            return HashCode;
        }
    }
}
