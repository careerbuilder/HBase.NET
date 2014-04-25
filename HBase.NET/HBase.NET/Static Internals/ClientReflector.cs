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
using System.Reflection;
using Castle.DynamicProxy;

namespace Hbase.StaticInternals
{
    internal class ClientReflector
    {
        private const BindingFlags BINDINGATTRIBUTES = BindingFlags.Instance | BindingFlags.Public | BindingFlags.SetProperty | BindingFlags.GetProperty;
        private const BindingFlags INVOKINGATTRIBUTES = BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.InvokeMethod;
        private static ProxyGenerator _ProxyGen = new ProxyGenerator();

        internal static IEnumerable<PropertyInfo> GetPublicInstancePropertyInfo<POCO>()
            where POCO : class
        {
            return (typeof(POCO)).GetProperties(BINDINGATTRIBUTES).Where(p => p.CanRead && p.CanWrite).ToArray();
        }

        internal static IEnumerable<FieldInfo> GetPublicInstanceFieldInfo<POCO>()
            where POCO : class
        {
            return (typeof(POCO)).GetFields(BINDINGATTRIBUTES);
        }

        internal static List<byte[]> GetColumns<POCO>(DateTime StartTime, DateTime StopTime, TimeInterval Interval)
            where POCO : class
        {
            List<byte[]> Columns = new List<byte[]>();

            DateTime TimeStamp = StartTime;

            while (StopTime > TimeStamp)
            {
                Columns.AddRange(GetColumns<POCO>(TimeStamp, Interval));

                switch (Interval)
                {
                    case TimeInterval.Hourly:
                        TimeStamp = TimeStamp.AddHours(1.0);
                        break;
                    default:
                        throw new ArgumentException("The TimeInterval is invalid or unsupported");
                }
            }

            return Columns;
        }

        internal static List<byte[]> GetColumns<POCO>(DateTime TimeStamp, TimeInterval Interval)
            where POCO : class
        {
            List<byte[]> Columns = new List<byte[]>();

            foreach (PropertyInfo Info in GetPublicInstancePropertyInfo<POCO>())
            {
                Columns.Add(ClientEncoder.GetColumnFamilyColumnNameByteArray(Info.Name, TimeStamp, Interval));
            }

            foreach (FieldInfo Info in GetPublicInstanceFieldInfo<POCO>())
            {
                Columns.Add(ClientEncoder.GetColumnFamilyColumnNameByteArray(Info.Name, TimeStamp, Interval));
            }

            return Columns;
        }

        internal static List<byte[]> GetColumns<POCO>(string ColumnFamily)
            where POCO : class
        {
            List<byte[]> Columns = new List<byte[]>();

            foreach (PropertyInfo Info in GetPublicInstancePropertyInfo<POCO>())
            {
                Columns.Add(ClientEncoder.GetColumnFamilyColumnNameByteArray(ColumnFamily, Info.Name));
            }

            foreach (FieldInfo Info in GetPublicInstanceFieldInfo<POCO>())
            {
                Columns.Add(ClientEncoder.GetColumnFamilyColumnNameByteArray(ColumnFamily, Info.Name));
            }

            return Columns;
        }

        internal static IDictionary<DateTime, POCO> GetProxies<POCO>(IDictionary<byte[], TCell> Columns, TimeInterval Interval)
            where POCO : class
        {
            IDictionary<DateTime, POCO> Proxies = new Dictionary<DateTime, POCO>();

            foreach (KeyValuePair<DateTime, IDictionary<byte[], TCell>> kvp in ClientTimeSeriesGenerator.SegmentTimeSeriesColumns(Columns, Interval))
            {
                Proxies.Add(kvp.Key, GetProxy<POCO>(kvp.Value, true));
            }

            return Proxies;
        }

        internal static POCO GetProxy<POCO>(TRowResult Result)
            where POCO : class
        {
            return GetProxy<POCO>(Result.Columns, false);
        }

        internal static POCO GetProxy<POCO>(IDictionary<byte[], TCell> Columns, bool IsTimeSeries)
            where POCO : class
        {
            POCO Proxy;

            if (typeof(POCO).IsInterface)
            {
                Proxy = _ProxyGen.CreateInterfaceProxyWithoutTarget<POCO>(new PropertyInterceptor());
            }
            else
            {
                Proxy = Activator.CreateInstance<POCO>();
            }

            foreach (KeyValuePair<byte[], TCell> kvp in Columns)
            {
                string ColumnWithFamily = ClientEncoder.DecodeString(kvp.Key);
                string Name;

                if (IsTimeSeries)
                {
                    Name = ClientEncoder.GetColumnFamily(ColumnWithFamily);
                }
                else
                {
                    Name = ClientEncoder.GetColumnName(ColumnWithFamily);
                }

                MemberInfo Info = typeof(POCO).GetProperty(Name, BINDINGATTRIBUTES);

                if ((object)Info == null)
                {
                    Info = typeof(POCO).GetField(Name, BINDINGATTRIBUTES);
                }

                object Value = TryGetValueByMemberInfo(Info, kvp.Value.Value);

                if ((object)Value != null)
                {
                    SetInfoValue(Info, Proxy, Value);
                }
            }

            return Proxy;
        }

        private static object TryGetValueByMemberInfo(MemberInfo Info, byte[] Value)
        {
            Type InfoType = null;
            PropertyInfo PInfo = Info as PropertyInfo;

            if ((object)PInfo == null)
            {
                FieldInfo FInfo = Info as FieldInfo;

                if ((object)FInfo != null)
                {
                    InfoType = FInfo.FieldType;
                }
            }
            else
            {
                InfoType = PInfo.PropertyType;
            }

            return TryGetValueByType(InfoType, Value);
        }

        internal static object TryGetValueByType(Type TInjected, byte[] Value)
        {
            object[] Params = new object[] { Value };

            MethodInfo Method;

            if ((object)TInjected != null && TInjected.IsValueType)
            {
                Method = typeof(ClientEncoder).GetMethod("TryGetValueForStructureByTypeCode", INVOKINGATTRIBUTES);
            }
            else
            {
                Method = typeof(ClientEncoder).GetMethod("TryGetValueForObjectByTypeCode", INVOKINGATTRIBUTES);
            }

            MethodInfo GenericMethod = Method.MakeGenericMethod(TInjected);

            return GenericMethod.Invoke(null, Params);
        }

        internal static List<Mutation> GetMutations(IHBaseMutation Mutation)
        {
            object[] Params = new object[] { Mutation.ColumnFamily, Mutation.Value, Mutation.IsDelete };

            MethodInfo Method = typeof(ClientMutator).GetMethod("GetMutations", INVOKINGATTRIBUTES);
            MethodInfo GenericMethod = Method.MakeGenericMethod(Mutation.ValueType);

            return (List<Mutation>)GenericMethod.Invoke(null, Params);
        }

        private static void SetInfoValue<POCO>(MemberInfo Info, POCO Proxy, object Value)
            where POCO : class
        {
            PropertyInfo PInfo = Info as PropertyInfo;

            if ((object)PInfo == null)
            {
                FieldInfo FInfo = Info as FieldInfo;

                if ((object)FInfo != null)
                {
                    FInfo.SetValue(Proxy, Value);
                }
            }
            else
            {
                PInfo.SetValue(Proxy, Value, null);
            }
        }
    }
}
