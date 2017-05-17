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
using System.Globalization;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Hbase.StaticInternals
{
    internal class ClientEncoder
    {
        //This, along with some massaging of the date is the format supported by sqoop. There will be a minimal amount of narrowing of the data; however,
        //the data which is being narrowed is often less percise than the system clock.
        internal const string DATEFORMAT = "yyyy-MM-dd HH:mm:ss.FFF";
        internal const char COLUMNFAMILYSEPERATOR = ':';
        internal const string DATEREQUIREDSUFFIX = ".0";
        internal const byte COLUMNFAMILYSEPERATORBYTE = 0x3A;

        internal static byte[] GetColumnFamilyColumnNameByteArray(string ColumnFamily, DateTime TimeStamp, TimeInterval Interval)
        {
            return GetColumnFamilyColumnNameByteArray(typeof(string), ColumnFamily,
                ClientTimeSeriesGenerator.GetTimeSeriesColumn(TimeStamp, Interval));
        }

        internal static byte[] GetColumnFamilyColumnNameByteArray(string ColumnFamily, string ColumnName)
        {
            return GetColumnFamilyColumnNameByteArray(typeof(string), ColumnFamily, ColumnName);
        }

        internal static byte[] GetColumnFamilyColumnNameByteArray(Type TInjectedColumn, string ColumnFamily, object ColumnName)
        {
            byte[] ColumnFamilyWithSeperator = EncodeString(ColumnFamily + COLUMNFAMILYSEPERATOR);

            return ColumnFamilyWithSeperator.Concat(GetBytesByType(TInjectedColumn, ColumnName)).ToArray();
        }

        internal static string GetColumnName(string ColumnWithFamily)
        {
            return ColumnWithFamily.Substring(ColumnWithFamily.IndexOf(COLUMNFAMILYSEPERATOR) + 1);
        }

        internal static string GetColumnName(byte[] ColumnWithFamily)
        {
            return GetColumnName(DecodeString(ColumnWithFamily));
        }

        internal static byte[] GetColumnNameByteArray(byte[] ColumnWithFamily)
        {
            byte[] ReturnArray;
            int Index = Array.IndexOf(ColumnWithFamily, COLUMNFAMILYSEPERATORBYTE);

            if (Index == -1)
            {
                ReturnArray = new byte[0];
            }
            else
            {
                ReturnArray = ColumnWithFamily.Skip(Index + 1).ToArray();
            }

            return ReturnArray;
        }

        internal static string GetColumnFamily(string ColumnWithFamily)
        {
            return ColumnWithFamily.Substring(0, ColumnWithFamily.IndexOf(COLUMNFAMILYSEPERATOR));
        }

        internal static string GetColumnFamily(byte[] ColumnWithFamily)
        {
            return DecodeString(ColumnWithFamily.TakeWhile(f =>
            {
                return f != COLUMNFAMILYSEPERATORBYTE;
            }).ToArray()); 
        }

        internal static byte[] EncodeString(string Input)
        {
            return Encoding.UTF8.GetBytes(Input);
        }

        private static byte[] EncodeDate(DateTime Input)
        {
            string DateString = ((DateTime)Input).ToString(DATEFORMAT, CultureInfo.InvariantCulture);

            if (!DateString.Contains('.'))
            {
                DateString += DATEREQUIREDSUFFIX;
            }

            return EncodeString(DateString);
        }

        internal static string DecodeString(byte[] Input)
        {
            return Encoding.UTF8.GetString(Input);
        }

        private static decimal? DecodeDecimal(byte[] Input)
        {
            string DecimalString = DecodeString(Input);

            decimal ParsedDecimal;
            decimal? ReturnValue = null;

            if (decimal.TryParse(DecimalString, out ParsedDecimal))
            {
                ReturnValue = ParsedDecimal;
            }

            return ReturnValue;
        }

        internal static DateTime? DecodeDate(byte[] Input)
        {
            string DateString = DecodeString(Input);

            if (DateString.EndsWith(DATEREQUIREDSUFFIX))
            {
                DateString = DateString.Substring(0, DateString.Length - 2);
            }

            return TryParseDate(DateString, DATEFORMAT);
        }

        internal static DateTime? TryParseDate(string DateString, string Format)
        {
            DateTime ParsedDate;
            DateTime? ReturnValue = null;

            if (DateTime.TryParseExact(DateString, Format, CultureInfo.InvariantCulture, DateTimeStyles.None, out ParsedDate))
            {
                ReturnValue = ParsedDate;
            }

            return ReturnValue;
        }

        internal static byte[] GetBytesByType(Type TInjectedValue, object Value)
        {
            TypeCode Code = Type.GetTypeCode(TInjectedValue);
            byte[] ReturnBytes = null;

            switch (Code)
            {
                case TypeCode.Boolean:
                    ReturnBytes = new byte[] { Convert.ToByte((bool)Value) };

                    break;
                case TypeCode.Byte:
                    ReturnBytes = new byte[] { (byte)Value };

                    break;
                case TypeCode.Char:
                    ReturnBytes = EncodeString(((char)Value).ToString());

                    break;
                case TypeCode.DateTime:
                    ReturnBytes = EncodeDate((DateTime)Value);

                    break;
                case TypeCode.Decimal:
                    ReturnBytes = EncodeString(((decimal)Value).ToString());

                    break;
                case TypeCode.Double:
                    ReturnBytes = Reverse(BitConverter.GetBytes((double)Value));

                    break;
                case TypeCode.Int16:
                    ReturnBytes = Reverse(BitConverter.GetBytes((short)Value));

                    break;
                case TypeCode.Int32:
                    ReturnBytes = Reverse(BitConverter.GetBytes((int)Value));

                    break;
                case TypeCode.Int64:
                    ReturnBytes = Reverse(BitConverter.GetBytes((long)Value));

                    break;
                case TypeCode.SByte:
                    unchecked
                    {
                        ReturnBytes = new byte[] { (byte)(sbyte)Value };
                    }

                    break;
                case TypeCode.Single:
                    ReturnBytes = Reverse(BitConverter.GetBytes((float)Value));

                    break;
                case TypeCode.String:
                    ReturnBytes = EncodeString((string)Value);

                    break;
                case TypeCode.UInt16:
                    ReturnBytes = Reverse(BitConverter.GetBytes((ushort)Value));

                    break;
                case TypeCode.UInt32:
                    ReturnBytes = Reverse(BitConverter.GetBytes((uint)Value));

                    break;
                case TypeCode.UInt64:
                    ReturnBytes = Reverse(BitConverter.GetBytes((ulong)Value));

                    break;

                case TypeCode.Object:
                    if (TInjectedValue == typeof(byte[]))
                    {
                        ReturnBytes = (byte[])Value;
                    }   //This is to support nullables
                    else if (TInjectedValue.IsGenericType && TInjectedValue.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        if ((Object)Value != null)
                        {
                            ReturnBytes = GetBytesByType(TInjectedValue.GetGenericArguments().First(), Value);
                        }
                    }
                    else
                    {
                        using (var mem = new MemoryStream())
                        {
                            var bf = new BinaryFormatter();
                            bf.Serialize(mem, Value);
                            ReturnBytes = mem.ToArray();
                        }
                    }

                    break;
            }

            return ReturnBytes;
        }

        private static T TryGetValueForObjectByTypeCode<T>(byte[] Value)
            where T : class
        {
            TypeCode Code = Type.GetTypeCode(typeof(T));
            T ReturnValue = null;

            if ((object)Value != null && Value.Length > 0)
            {
                try
                {
                    switch (Code)
                    {
                        case TypeCode.String:
                            ReturnValue = (T)(object)DecodeString(Value);

                            break;
                        case TypeCode.Object:
                            if (typeof(T) == typeof(byte[]))
                            {
                                ReturnValue = (T)(object)Value;
                            }
                            else
                            {
                                using (var mem = new MemoryStream(Value))
                                {
                                    var bf = new BinaryFormatter();
                                    ReturnValue = (T) bf.Deserialize(mem);
                                }
                            }

                            break;
                    }
                }
                catch
                {
                    ReturnValue = null;
                }
            }

            return ReturnValue;
        }

        private static T? TryGetValueForStructureByTypeCode<T>(byte[] Value)
            where T : struct
        {
            TypeCode Code = Type.GetTypeCode(typeof(T));
            T? ReturnValue = null;

            if ((object)Value != null && Value.Length > 0)
            {
                try
                {
                    switch (Code)
                    {
                        case TypeCode.Boolean:
                            ReturnValue = (T)(object)Convert.ToBoolean(Value.FirstOrDefault());

                            break;
                        case TypeCode.Byte:
                            ReturnValue = (T)(object)Value.FirstOrDefault();

                            break;
                        case TypeCode.Char:
                            ReturnValue = (T)(object)DecodeString(Value).FirstOrDefault();

                            break;
                        case TypeCode.DateTime:
                            ReturnValue = (T)(object)DecodeDate(Value);

                            break;
                        case TypeCode.Decimal:
                            ReturnValue = (T)(object)DecodeDecimal(Value);

                            break;
                        case TypeCode.Double:
                            ReturnValue = (T)(object)BitConverter.ToDouble(Reverse(Value), 0);

                            break;
                        case TypeCode.Int16:
                            ReturnValue = (T)(object)BitConverter.ToInt16(Reverse(Value), 0);

                            break;
                        case TypeCode.Int32:
                            ReturnValue = (T)(object)BitConverter.ToInt32(Reverse(Value), 0);

                            break;
                        case TypeCode.Int64:
                            ReturnValue = (T)(object)BitConverter.ToInt64(Reverse(Value), 0);

                            break;
                        case TypeCode.SByte:
                            unchecked
                            {
                                ReturnValue = (T)(object)(sbyte)Value.FirstOrDefault();
                            }

                            break;
                        case TypeCode.Single:
                            ReturnValue = (T)(object)BitConverter.ToSingle(Reverse(Value), 0);

                            break;
                        case TypeCode.UInt16:
                            ReturnValue = (T)(object)BitConverter.ToUInt16(Reverse(Value), 0);

                            break;
                        case TypeCode.UInt32:
                            ReturnValue = (T)(object)BitConverter.ToUInt32(Reverse(Value), 0);

                            break;
                        case TypeCode.UInt64:
                            ReturnValue = (T)(object)BitConverter.ToUInt64(Reverse(Value), 0);

                            break;
                    }
                }
                catch
                {
                    ReturnValue = null;
                }
            }

            return ReturnValue;
        }

        private static byte[] Reverse(byte[] Input)
        {
            if ((object)Input != null)
            {
                Array.Reverse(Input);
            }

            return Input;
        }
    }
}
