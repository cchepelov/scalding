/*
Copyright 2012 Twitter, Inc.

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
package com.twitter.scalding.typed

import scala.reflect.ClassTag
import com.twitter.algebird._
import com.twitter.scalding.{ Mode, IterableSource }

import com.twitter.scalding.Execution

object ValuePipe extends java.io.Serializable {
  implicit def toTypedPipe[V](v: ValuePipe[V]): TypedPipe[V] = v.toTypedPipe

  def fold[T, U, V](l: ValuePipe[T], r: ValuePipe[U])(f: (T, U) => V)(implicit mfU: ClassTag[U], mfV: ClassTag[V]): ValuePipe[V] =
    l.leftCross(r).collect { case (t, Some(u)) => f(t, u) }

  def apply[T](t: T)(implicit mfT: ClassTag[T]): ValuePipe[T] = LiteralValue(t)
  def empty: ValuePipe[Nothing] = EmptyValue
}

/**
 * ValuePipe is special case of a TypedPipe of just a optional single element.
 *  It is like a distribute Option type
 * It allows to perform scalar based operations on pipes like normalization.
 */
sealed trait ValuePipe[+T] extends java.io.Serializable {
  implicit def valueManifest: ClassTag[_ <: T]

  def leftCross[U](that: ValuePipe[U])(implicit mfU: ClassTag[U]): ValuePipe[(T, Option[U])] = {
    implicit val mfT = valueManifest.asInstanceOf[ClassTag[T]]

    that match {
      case EmptyValue => map((_, None))
      case LiteralValue(v2) => map((_, Some(v2)))
      // We don't know if a computed value is empty or not. We need to run the MR job:
      case _ => ComputedValue(toTypedPipe.leftCross(that))
    }
  }
  def collect[U](fn: PartialFunction[T, U])(implicit mfU: ClassTag[U]): ValuePipe[U] =
    filter(fn.isDefinedAt(_)).map(fn(_))

  def map[U](fn: T => U)(implicit mfU: ClassTag[U]): ValuePipe[U]
  def filter(fn: T => Boolean): ValuePipe[T]
  /**
   * Identical to toOptionExecution.map(_.get)
   * The result will be an exception if there is no value.
   * The name here follows the convention of adding
   * Execution to the name so in the repl in is removed
   */
  def getExecution: Execution[T] = toOptionExecution.map(_.get)
  /**
   * Like the above, but with a lazy parameter that is evaluated
   * if the value pipe is empty
   * The name here follows the convention of adding
   * Execution to the name so in the repl in is removed
   */
  def getOrElseExecution[U >: T](t: => U): Execution[U] = toOptionExecution.map(_.getOrElse(t))
  def toTypedPipe: TypedPipe[T]

  /**
   * Convert this value to an Option. It is an error if somehow
   * this is not either empty or has one value.
   * The name here follows the convention of adding
   * Execution to the name so in the repl in is removed
   */
  def toOptionExecution: Execution[Option[T]] =
    toTypedPipe.toIterableExecution.map { it =>
      it.iterator.take(2).toList match {
        case Nil => None
        case h :: Nil => Some(h)
        case items => sys.error("More than 1 item in an ValuePipe: " + items.toString)
      }
    }

  def debug: ValuePipe[T]
}
case object EmptyValue extends ValuePipe[Nothing] {
  def valueManifest: ClassTag[_ <: Nothing] = Manifest.classType[Nothing](classOf[Nothing])

  override def leftCross[U](that: ValuePipe[U])(implicit mfU: ClassTag[U]) = this
  override def map[U](fn: Nothing => U)(implicit mfU: ClassTag[U]): ValuePipe[U] = this
  override def filter(fn: Nothing => Boolean) = this
  override def toTypedPipe: TypedPipe[Nothing] = TypedPipe.empty
  override def toOptionExecution = Execution.from(None)

  def debug: ValuePipe[Nothing] = {
    println("EmptyValue")
    this
  }
}
case class LiteralValue[T](value: T)(implicit override val valueManifest: ClassTag[T]) extends ValuePipe[T] {
  override def map[U](fn: T => U)(implicit mfU: ClassTag[U]) = LiteralValue(fn(value))
  override def filter(fn: T => Boolean) = if (fn(value)) this else EmptyValue
  override def toTypedPipe = TypedPipe.from(Iterable(value))
  override def toOptionExecution = Execution.from(Some(value))

  def debug: ValuePipe[T] = map { (v: T) =>
    println("LiteralValue(" + v.toString + ")")
    v
  }
}
case class ComputedValue[T](override val toTypedPipe: TypedPipe[T]) extends ValuePipe[T] {
  implicit override def valueManifest: ClassTag[T] = toTypedPipe.valueManifest.asInstanceOf[ClassTag[T]]

  override def map[U](fn: T => U)(implicit mfU: ClassTag[U]) = ComputedValue(toTypedPipe.map(fn))
  override def filter(fn: T => Boolean) = ComputedValue(toTypedPipe.filter(fn))

  def debug: ValuePipe[T] =
    map { (value: T) =>
      println("ComputedValue(" + value.toString + ")")
      value
    }
}
