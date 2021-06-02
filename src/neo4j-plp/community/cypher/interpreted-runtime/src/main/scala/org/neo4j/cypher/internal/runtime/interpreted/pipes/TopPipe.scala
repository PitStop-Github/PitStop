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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import java.util.Comparator

import org.neo4j.cypher.internal.DefaultComparatorTopTable
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.values.storable.NumberValue

import scala.collection.JavaConverters._
import scala.collection.mutable

/*
 * TopPipe is used when a query does a ORDER BY ... LIMIT query. Instead of ordering the whole result set and then
 * returning the matching top results, we only keep the top results in heap, which allows us to release memory earlier
 */
abstract class TopPipe(source: Pipe, comparator: Comparator[ExecutionContext]) extends PipeWithSource(source)

case class TopNPipe(source: Pipe, countExpression: Expression, comparator: Comparator[ExecutionContext])
                   (val id: Id = Id.INVALID_ID) extends TopPipe(source, comparator: Comparator[ExecutionContext]) {

  countExpression.registerOwningPipe(this)

  private val initialFallbackSortArraySize = Int.MaxValue/8 // This should not be too big so as to risk out-of-memory on the first allocation

  protected override def internalCreateResults(input:Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    if (input.isEmpty) Iterator.empty
    else {
      val first = input.next()
      val longCount = countExpression(first, state).asInstanceOf[NumberValue].longValue()
      if (longCount <= 0) {
        Iterator.empty
      }
      else if (longCount > Int.MaxValue) {
        // For count values larger than the maximum 32-bit integer we fallback on a full sort instead of allocating a huge top table
        // (Instead of throw new IllegalArgumentException(s"ORDER BY + LIMIT $longCount exceeds the maximum value of ${Int.MaxValue}"))
        // NOTE: If the _input size_ is larger than Int.MaxValue this will still fail, since an array cannot hold that many elements
        val buffer = new mutable.ArrayBuffer[ExecutionContext](initialFallbackSortArraySize)
        buffer += first
        buffer ++= input
        val array = buffer.toArray
        java.util.Arrays.sort(array, comparator)
        var c: Long = 0 // Counter to be used inside of stream
        array.toStream.takeWhile { _ => c = c + 1; c <= longCount }.iterator
      }
      else {
        // The main case: allocate a table of size count to hold the top rows
        val count = longCount.toInt
        val topTable = new DefaultComparatorTopTable(comparator, count)
        topTable.add(first)

        input.foreach {
          ctx =>
            topTable.add(ctx)
        }

        topTable.sort()

        topTable.iterator.asScala
      }
    }
  }
}

/*
 * Special case for when we only have one element, in this case it is no idea to store
 * an array, instead just store a single value.
 */
case class Top1Pipe(source: Pipe, comparator: Comparator[ExecutionContext])
                   (val id: Id = Id.INVALID_ID)
  extends TopPipe(source, comparator: Comparator[ExecutionContext]) {

  protected override def internalCreateResults(input: Iterator[ExecutionContext],
                                               state: QueryState): Iterator[ExecutionContext] = {
    if (input.isEmpty) Iterator.empty
    else {

      val first = input.next()
      var result = first

      input.foreach {
        ctx =>
          if (comparator.compare(ctx, result) < 0) {
            result = ctx
          }
      }
      Iterator.single(result)
    }
  }
}

/*
 * Special case for when we only want one element, and all others that have the same value (tied for first place)
 */
case class Top1WithTiesPipe(source: Pipe, comparator: Comparator[ExecutionContext])
                           (val id: Id = Id.INVALID_ID)
  extends TopPipe(source, comparator: Comparator[ExecutionContext]) {

  protected override def internalCreateResults(input: Iterator[ExecutionContext],
                                               state: QueryState): Iterator[ExecutionContext] = {
    if (input.isEmpty)
      Iterator.empty
    else {
      val first = input.next()
      var best = first
      var matchingRows = init(best)

      input.foreach {
        ctx =>
          val comparison = comparator.compare(ctx, best)
          if (comparison < 0) { // Found a new best
            best = ctx
            matchingRows.clear()
            matchingRows += ctx
          }

          if (comparison == 0) { // Found a tie
            matchingRows += ctx
          }
      }
      matchingRows.result().iterator
    }
  }

  @inline
  private def init(first: ExecutionContext) = {
    val builder = Vector.newBuilder[ExecutionContext]
    builder += first
    builder
  }
}
