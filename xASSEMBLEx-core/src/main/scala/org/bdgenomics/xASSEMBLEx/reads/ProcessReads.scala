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
package org.bdgenomics.xASSEMBLEx.reads

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.formats.avro.ADAMRecord
import org.bdgenomics.xASSEMBLEx.debrujin.{ Qmer, MergedQmer }

object ProcessReads {

  /**
   * Chops reads into qmers, and merges all qmers with the same kmer string.
   *
   * @param rdd An RDD of reads.
   * @param qmerLength q (k) length for q-mers.
   * @return Returns an RDD of qmers.
   */
  def apply(rdd: RDD[ADAMRecord],
            qmerLength: Int): RDD[MergedQmer] = {
    // serializable processing class
    val pr = new ProcessReads

    // cut reads into qmers, and combine qmers
    rdd.flatMap(pr.readToQmers(_, qmerLength))
      .groupBy(_.key)
      .map(p => MergedQmer(p._2))
  }

}

private[reads] class ProcessReads extends Serializable {

  /**
   * Cuts a single read into q-mers.
   *
   * @param read Read to cut.
   * @param qmerLength The length of the qmer to cut.
   * @return Returns an iterator containing q-mer/weight mappings.
   */
  def readToQmers(read: ADAMRecord,
                  qmerLength: Int): Iterator[Qmer] = {
    // get read bases and quality scores
    val bases = read.getSequence.toString.toCharArray
    val scores = read.getQual.toString.toCharArray.map(q => {
      PhredUtils.phredToSuccessProbability(q - 33)
    })

    // zip and put into sliding windows to get qmers
    bases.zip(scores)
      .sliding(qmerLength + 1)
      .map(w => {
        // bases are first in tuple
        val b = w.map(_._1)

        // quals are second
        val q = w.map(_._2)

        // reduce bases into string, reduce quality scores
        Qmer(b.dropRight(1).foldLeft("")(_ + _),
          q.dropRight(1).reduce(_ * _),
          b.last,
          q.last)
      })
  }
}
