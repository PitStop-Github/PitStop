/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.values._
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable._
import org.neo4j.cypher.internal.v3_5.util.CypherTypeException
import org.neo4j.cypher.internal.v3_5.util.symbols._

abstract class MathFunction(arg: Expression) extends Expression with NumericHelper {

  def innerExpectedType = CTNumber

  override def arguments = Seq(arg)

  override def symbolTableDependencies = arg.symbolTableDependencies
}

abstract class NullSafeMathFunction(arg: Expression) extends MathFunction(arg) {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val value = arg(ctx, state)
    if (NO_VALUE == value) NO_VALUE else Values.doubleValue(apply(asDouble(value).doubleValue()))
  }

  def apply(value: Double): Double
}

trait NumericHelper {

  protected def asLongEntityId(a: AnyValue): Option[Long] = a match {
    case a: IntegralValue => Some(a.longValue())
    case _ => None
  }

  protected def asDouble(a: AnyValue): DoubleValue = Values.doubleValue(asNumber(a).doubleValue())

  protected def asInt(a: AnyValue): IntValue = Values.intValue(asPrimitiveInt(a))

  protected def asPrimitiveInt(a: AnyValue): Int = asNumber(a).longValue().toInt

  protected def asLong(a: AnyValue): LongValue = Values.longValue(asPrimitiveLong(a))

  protected def asPrimitiveLong(a: AnyValue): Long = asNumber(a).longValue()

  private def asNumber(a: AnyValue): NumberValue = a match {
    case null => throw new CypherTypeException("Expected a numeric value for " + toString + ", but got null")
    case NO_VALUE => throw new CypherTypeException("Expected a numeric value for " + toString + ", but got null")
    case n: NumberValue => n
    case _ => throw new CypherTypeException("Expected a numeric value for " + toString + ", but got: " + a.toString)
  }
}

case class AbsFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val value = argument(ctx, state)
    if (value == NO_VALUE) NO_VALUE else CypherFunctions.abs(value)
  }

  override def rewrite(f: (Expression) => Expression) = f(AbsFunction(argument.rewrite(f)))
}

case class AcosFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext,
                     state: QueryState): AnyValue = argument(ctx, state) match {
    case NO_VALUE => NO_VALUE
    case v => CypherFunctions.acos(v)
  }

  override def rewrite(f: (Expression) => Expression) = f(AcosFunction(argument.rewrite(f)))
}

case class AsinFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext,
                     state: QueryState): AnyValue = argument(ctx, state) match {
    case NO_VALUE => NO_VALUE
    case v => CypherFunctions.asin(v)
  }

  override def rewrite(f: (Expression) => Expression) = f(AsinFunction(argument.rewrite(f)))
}

case class AtanFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext,
                     state: QueryState): AnyValue = argument(ctx, state) match {
    case NO_VALUE => NO_VALUE
    case v => CypherFunctions.atan(v)
  }

  override def rewrite(f: (Expression) => Expression) = f(AtanFunction(argument.rewrite(f)))
}

case class Atan2Function(y: Expression, x: Expression) extends Expression with NumericHelper {

  def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val yValue = y(ctx, state)
    val xValue = x(ctx, state)
    if (NO_VALUE == yValue || NO_VALUE == xValue)
      NO_VALUE
    else
     CypherFunctions.atan2(yValue, xValue)
  }

  override def arguments = Seq(x, y)

  override def rewrite(f: (Expression) => Expression) = f(Atan2Function(y.rewrite(f), x.rewrite(f)))

  override def symbolTableDependencies = x.symbolTableDependencies ++ y.symbolTableDependencies
}

case class CeilFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext,
                     state: QueryState): AnyValue = argument(ctx, state) match {
    case NO_VALUE => NO_VALUE
    case v => CypherFunctions.ceil(v)
  }

  override def rewrite(f: (Expression) => Expression) = f(CeilFunction(argument.rewrite(f)))
}

case class CosFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext,
                     state: QueryState): AnyValue = argument(ctx, state) match {
    case NO_VALUE => NO_VALUE
    case v => CypherFunctions.cos(v)
  }

  override def rewrite(f: (Expression) => Expression) = f(CosFunction(argument.rewrite(f)))
}

case class CotFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext,
                     state: QueryState): AnyValue = argument(ctx, state) match {
    case NO_VALUE => NO_VALUE
    case v => CypherFunctions.cot(v)
  }

  override def rewrite(f: (Expression) => Expression) = f(CotFunction(argument.rewrite(f)))
}

case class DegreesFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext,
                     state: QueryState): AnyValue = argument(ctx, state) match {
    case NO_VALUE => NO_VALUE
    case v => CypherFunctions.toDegrees(v)
  }

  override def rewrite(f: (Expression) => Expression) = f(DegreesFunction(argument.rewrite(f)))
}

case class EFunction() extends Expression() {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = Values.E

  override def arguments = Seq()

  override def symbolTableDependencies = Set[String]()

  override def rewrite(f: (Expression) => Expression) = f(EFunction())
}

case class ExpFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext,
                     state: QueryState): AnyValue = argument(ctx, state) match {
    case NO_VALUE => NO_VALUE
    case v => CypherFunctions.exp(v)
  }

  override def rewrite(f: (Expression) => Expression) = f(ExpFunction(argument.rewrite(f)))
}

case class FloorFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext,
                     state: QueryState): AnyValue = argument(ctx, state) match {
    case NO_VALUE => NO_VALUE
    case v => CypherFunctions.floor(v)
  }

  override def rewrite(f: (Expression) => Expression) = f(FloorFunction(argument.rewrite(f)))
}

case class LogFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext,
                     state: QueryState): AnyValue = argument(ctx, state) match {
    case NO_VALUE => NO_VALUE
    case v => CypherFunctions.log(v)
  }

  override def rewrite(f: (Expression) => Expression) = f(LogFunction(argument.rewrite(f)))
}

case class Log10Function(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext,
                     state: QueryState): AnyValue = argument(ctx, state) match {
    case NO_VALUE => NO_VALUE
    case v => CypherFunctions.log10(v)
  }

  override def rewrite(f: (Expression) => Expression) = f(Log10Function(argument.rewrite(f)))
}

case class PiFunction() extends Expression {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = Values.PI

  override def arguments = Seq()

  override def symbolTableDependencies = Set()

  override def rewrite(f: (Expression) => Expression) = f(PiFunction())
}

case class RadiansFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext,
                     state: QueryState): AnyValue = argument(ctx, state) match {
    case NO_VALUE => NO_VALUE
    case v => CypherFunctions.toRadians(v)
  }

  override def rewrite(f: (Expression) => Expression) = f(RadiansFunction(argument.rewrite(f)))
}

case class SinFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext,
                     state: QueryState): AnyValue = argument(ctx, state) match {
    case NO_VALUE => NO_VALUE
    case v => CypherFunctions.sin(v)
  }

  override def rewrite(f: (Expression) => Expression) = f(SinFunction(argument.rewrite(f)))
}

case class HaversinFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext,
                     state: QueryState): AnyValue = argument(ctx, state) match {
    case NO_VALUE => NO_VALUE
    case v => CypherFunctions.haversin(v)
  }

  override def rewrite(f: (Expression) => Expression) = f(HaversinFunction(argument.rewrite(f)))
}

case class TanFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext,
                     state: QueryState): AnyValue = argument(ctx, state) match {
    case NO_VALUE => NO_VALUE
    case v => CypherFunctions.tan(v)
  }

  override def rewrite(f: (Expression) => Expression) = f(TanFunction(argument.rewrite(f)))
}

case class RandFunction() extends Expression {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = CypherFunctions.rand()

  override def arguments = Seq()

  override def symbolTableDependencies = Set[String]()

  override def rewrite(f: (Expression) => Expression) = f(RandFunction())
}

case class RangeFunction(start: Expression, end: Expression, step: Expression) extends Expression with NumericHelper {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue =
    CypherFunctions.range(start(ctx, state), end(ctx, state), step(ctx, state))

  override def arguments = Seq(start, end, step)

  override def rewrite(f: (Expression) => Expression) =
    f(RangeFunction(start.rewrite(f), end.rewrite(f), step.rewrite(f)))

  override def symbolTableDependencies = start.symbolTableDependencies ++
    end.symbolTableDependencies ++
    step.symbolTableDependencies
}

case class SignFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val value = argument(ctx, state)
    if (NO_VALUE == value) NO_VALUE
    else {
      Values.longValue(Math.signum(asDouble(value).doubleValue()).toLong)
    }
  }

  override def rewrite(f: (Expression) => Expression) = f(SignFunction(argument.rewrite(f)))
}

case class RoundFunction(expression: Expression) extends MathFunction(expression) {

  override def apply(ctx: ExecutionContext,
                     state: QueryState): AnyValue = expression(ctx, state) match {
    case NO_VALUE => NO_VALUE
    case v => CypherFunctions.round(v)
  }

  override def rewrite(f: (Expression) => Expression) = f(RoundFunction(expression.rewrite(f)))
}

case class SqrtFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext,
                     state: QueryState): AnyValue = argument(ctx, state) match {
    case NO_VALUE => NO_VALUE
    case v => CypherFunctions.sqrt(v)
  }

  override def rewrite(f: (Expression) => Expression) = f(SqrtFunction(argument.rewrite(f)))
}
