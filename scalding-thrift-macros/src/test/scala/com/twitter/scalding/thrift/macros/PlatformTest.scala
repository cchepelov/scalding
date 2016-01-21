/*
Copyright 2014 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.scalding.thrift.macros

import com.twitter.scalding._
import com.twitter.scalding.platform.{ HadoopPlatformJobTest, HadoopSharedPlatformTest }
import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scalding.thrift.macros.impl.ScroogeInternalOrderedSerializationImpl
import com.twitter.scalding.thrift.macros.scalathrift._

import org.scalacheck.Arbitrary
import org.scalatest.{ Matchers, WordSpec }

import scala.reflect.ClassTag
import scala.language.experimental.{ macros => sMacros }

class CompareJob[T: OrderedSerialization](in: Iterable[T], args: Args)(implicit mfT: ClassTag[T]) extends Job(args) {
  TypedPipe.from(in).flatMap{ i =>
    (0 until 1).map (_ => i)
  }.map(_ -> 1L).sumByKey.map {
    case (k, v) =>
      (k.hashCode, v)
  }.write(TypedTsv[(Int, Long)]("output"))
}
private[macros] trait InstanceProvider[T] {
  def g(idx: Int): T
}
class PlatformTest extends WordSpec with Matchers with HadoopSharedPlatformTest {
  org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.FATAL)
  org.apache.log4j.Logger.getLogger("org.mortbay").setLevel(org.apache.log4j.Level.FATAL)
  implicit def toScroogeInternalOrderedSerialization[T]: OrderedSerialization[T] = macro ScroogeInternalOrderedSerializationImpl[T]

  import ScroogeGenerators._

  implicit def arbitraryInstanceProvider[T: Arbitrary] = new InstanceProvider[T] {
    def g(idx: Int) = ScroogeGenerators.dataProvider[T](idx)
  }

  def runCompareTest[T: OrderedSerialization](iprov: InstanceProvider[T])(implicit mfT: ClassTag[T]) {
    val input = (0 until 10000).map { idx =>
      iprov.g(idx % 50)
    }

    HadoopPlatformJobTest(new CompareJob[T](input, _), cluster)
      .sink(TypedTsv[(Int, Long)]("output")) { out =>
        val expected =
          input
            .groupBy(identity)
            .map{ case (k, v) => (k.hashCode, v.size) }

        out.toSet shouldBe expected.toSet
      }
      .run
  }

  "ThriftStruct Test" should {

    "Expected items should match : Internal Serializer / TestStructdd" in {
      toScroogeInternalOrderedSerialization[TestStruct]
      runCompareTest[TestStruct](arbitraryInstanceProvider[TestStruct])
    }

    "Expected items should match : Internal Serializer / TestSets" in {
      toScroogeInternalOrderedSerialization[TestSets]
      runCompareTest[TestSets](arbitraryInstanceProvider[TestSets])
    }

    "Expected items should match : Internal Serializer / TestLists" in {
      toScroogeInternalOrderedSerialization[TestLists]
      runCompareTest[TestLists](arbitraryInstanceProvider[TestLists])
    }

    "Expected items should match : Internal Serializer /  TestMaps" in {
      toScroogeInternalOrderedSerialization[TestMaps]
      runCompareTest[TestMaps](arbitraryInstanceProvider[TestMaps])
    }

    "Expected items should match : Internal Serializer / TestUnion" in {
      toScroogeInternalOrderedSerialization[TestUnion]
      runCompareTest[TestUnion](arbitraryInstanceProvider[TestUnion])
    }

    "Expected items should match : Internal Serializer / Enum" in {
      toScroogeInternalOrderedSerialization[TestEnum]
      runCompareTest[TestEnum](arbitraryInstanceProvider[TestEnum])
    }

    "Expected items should match : Internal Serializer / TestTypes" in {
      toScroogeInternalOrderedSerialization[TestTypes]
      runCompareTest[TestTypes](arbitraryInstanceProvider[TestTypes])
    }

    "Expected items should match : Internal Serializer / TestTypes2" in {
      toScroogeInternalOrderedSerialization[TestTypes]
      runCompareTest[TestTypes](arbitraryInstanceProvider[TestTypes])
    }

    "Expected items should match : Internal Serializer / (Long, TestTypes)" in {
      case object Container {
        def ord[T](implicit oSer: OrderedSerialization[T]): OrderedSerialization[(Long, T)] = {
          implicitly[OrderedSerialization[(Long, T)]]
        }
      }

      val ordSer = Container.ord[TestTypes]
      runCompareTest[(Long, TestTypes)](ordSer)
    }

  }

}
