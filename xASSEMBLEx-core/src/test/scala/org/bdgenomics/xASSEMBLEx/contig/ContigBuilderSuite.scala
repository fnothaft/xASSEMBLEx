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

import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.formats.avro.ADAMRecord
import org.bdgenomics.xASSEMBLEx.debrujin.{ DeBrujinGraph, MergedQmer }
import org.bdgenomics.xASSEMBLEx.reads.ProcessReads

class ContigBuilderSuite extends SparkFunSuite {

  sparkTest("reconstruct a single read into a contig") {
    val readSequence = "ACCCTGCGGCTCA"

    val read = ADAMRecord.newBuilder()
      .setSequence(readSequence)
      .setQual(".............")
      .build()

    val qmerRdd = ProcessReads(sc.parallelize(Seq(read)), 7)

    val graph = DeBrujinGraph(qmerRdd)
    val contigRdd = graph.buildContigs()

    assert(contigRdd.count === 1)
    assert(contigRdd.first.fragment === readSequence)
  }

}
