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

namespace Hbase.StaticInternals
{
    internal class ClientMutator
    {
        internal static List<Mutation> GetMutations<POCO>(string ColumnFamily, object Obj, bool IsDelete)
            where POCO : class
        {
            List<Mutation> Mutations = new List<Mutation>();

            AddMutationsForPublicInstanceFieldInfo<POCO>(Mutations, ColumnFamily, Obj, IsDelete);
            AddMutationsForPublicInstancePropertyInfo<POCO>(Mutations, ColumnFamily, Obj, IsDelete);

            return Mutations;
        }

        private static void AddMutationsForPublicInstanceFieldInfo<POCO>(IList<Mutation> Mutations, string ColumnFamily, object Obj, bool IsDelete)
            where POCO : class
        {
            foreach (FieldInfo Info in ClientReflector.GetPublicInstanceFieldInfo<POCO>())
            {
                if ((object)Obj == null)
                {
                    Mutations.Add(GetMutation(Obj, Info.FieldType, ColumnFamily, Info.Name, IsDelete));
                }
                else
                {
                    Mutations.Add(GetMutation(Info.GetValue(Obj), Info.FieldType, ColumnFamily, Info.Name, IsDelete));
                }
            }
        }

        private static void AddMutationsForPublicInstancePropertyInfo<POCO>(IList<Mutation> Mutations, string ColumnFamily, object Obj, bool IsDelete)
            where POCO : class
        {
            foreach (PropertyInfo Info in ClientReflector.GetPublicInstancePropertyInfo<POCO>())
            {
                if ((object)Obj == null)
                {
                    Mutations.Add(GetMutation(Obj, Info.PropertyType, ColumnFamily, Info.Name, IsDelete));
                }
                else
                {
                    Mutations.Add(GetMutation(Info.GetValue(Obj, null), Info.PropertyType, ColumnFamily, Info.Name, IsDelete));
                }
            }
        }

        private static Mutation GetMutation(object Value, Type ValueType, string ColumnFamily, string ColumnName, bool IsDelete)
        {
            Mutation m = new Mutation();

            m.Column = ClientEncoder.GetColumnFamilyColumnNameByteArray(ColumnFamily, ColumnName);
            m.IsDelete = IsDelete;

            if (!IsDelete)
            {
                if ((object)Value == null)
                {
                    m.IsDelete = true;
                }
                else
                {
                    m.Value = ClientEncoder.GetBytesByType(ValueType, Value);
                }
            }

            return m;
        }

        internal static List<BatchMutation> GetBatchMutations(IEnumerable<IHBaseMutation> Bulk)
        {
            List<BatchMutation> BatchMutations = new List<BatchMutation>();

            foreach (IHBaseMutation Mutation in Bulk)
            {
                BatchMutation Batch = new BatchMutation();
                Batch.Row = ClientEncoder.EncodeString(Mutation.Key);
                Batch.Mutations = ClientReflector.GetMutations(Mutation);

                BatchMutations.Add(Batch);
            }

            return BatchMutations;
        }
    }
}
