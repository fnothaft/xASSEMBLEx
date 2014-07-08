/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.xASSEMBLEx.contig

import org.bdgenomics.xASSEMBLEx.debrujin.MergedQmer
import scala.annotation.tailrec

object ContigBuilder extends Serializable {
  /**
   * Strings a set of connected q-mers into a contig.
   *
   * @param id The ID of this contig.
   * @param qmers The q-mers in this contig.
   * @return Returns an intermediate contig.
   */
  def apply(id: Long, qmers: Iterable[MergedQmer]): IntermediateContig = {

    // put qmers into sort order
    val sortedQmers = qmers.toSeq.sortBy(_.getRank)

    // get the contigs we can connect to from the first and last qmers
    val headAdjacentContigs = sortedQmers.head.getAdjacentContigs
    val tailAdjacentContigs = sortedQmers.last.getAdjacentContigs

    @tailrec def buildContigString(iter: Iterator[MergedQmer],
                                   cl: List[Char]): List[Char] = {
      if (!iter.hasNext) {
        // we've been building our string in reverse, so we need to reverse now
        cl.reverse
      } else {
        // get next qmer
        val qmer = iter.next

        // do we only have one possible continuation? if so, recurse, else return
        if (qmer.next.length != 1) {
          assert(!iter.hasNext,
            "We should have run out of q-mers (at " + qmer.id + "), but we have not. Out: " + qmer.receiveOut + " In: " + qmer.receiveIn)

          // we've been building our string in reverse, so we need to reverse now
          cl.reverse
        } else {
          buildContigString(iter, qmer.next(0) :: cl)
        }
      }
    }

    // build contig string
    val contigString = buildContigString(sortedQmers.toIterator,
      sortedQmers.head.kmer.toList.reverse)
      .foldLeft("")(_ + _.toString)

    // emit intermediate contig
    IntermediateContig(id, contigString, headAdjacentContigs, tailAdjacentContigs)
  }
}
