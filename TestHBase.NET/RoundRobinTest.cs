﻿//Copyright 2012 CareerBuilder, LLC. - http://www.careerbuilder.com

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
using Hbase.Iterators.RoundRobin;

namespace TestHBase.NET
{
    [TestClass]
    public class RoundRobinTest
    {
        [TestMethod]
        public void GetNextElement()
        {
            // Arrange
            List<int> elements = new List<int>();
            for (int i = 0; i < 10; i++)
            {
                elements.Add(i);
            }

            // Act
            RoundRobinIterator<int> iterator = new RoundRobinIterator<int>(elements);

            // Assert
            int rrIterator = 0;
            for (int i = 0; i < 25; i++)
            {
                Assert.AreEqual(rrIterator, iterator.Next());
                rrIterator = (rrIterator + 1) % elements.Count;
            }
        }
    }
}
