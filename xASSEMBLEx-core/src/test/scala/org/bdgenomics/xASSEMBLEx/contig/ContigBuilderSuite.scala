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

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
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

  sparkTest("reconstruct two distinct reads into separate contigs") {
    val readSequence1 = "ACCCTGCGGCTCA"
    val readSequence2 = "CACCACTGCGATT"

    val read1 = ADAMRecord.newBuilder()
      .setSequence(readSequence1)
      .setQual(".............")
      .build()
    val read2 = ADAMRecord.newBuilder()
      .setSequence(readSequence2)
      .setQual(".............")
      .build()

    val qmerRdd = ProcessReads(sc.parallelize(Seq(read1, read2)), 7)

    val graph = DeBrujinGraph(qmerRdd)
    val contigRdd = graph.buildContigs()

    assert(contigRdd.count === 2)
    assert(contigRdd.filter(_.fragment == readSequence1).count === 1)
    assert(contigRdd.filter(_.fragment == readSequence2).count === 1)
  }

  sparkTest("reconstruct three overlapping reads into a single contig") {
    val readSequence1 = "ACCCTGCGGCTCA"
    val readSequence2 = "CGGCTCACATTAT"
    val readSequence3 = "ACATTATTTACAC"

    val read1 = ADAMRecord.newBuilder()
      .setSequence(readSequence1)
      .setQual(".............")
      .build()
    val read2 = ADAMRecord.newBuilder()
      .setSequence(readSequence2)
      .setQual(".............")
      .build()
    val read3 = ADAMRecord.newBuilder()
      .setSequence(readSequence3)
      .setQual(".............")
      .build()

    val qmerRdd = ProcessReads(sc.parallelize(Seq(read1, read2, read3)), 7)

    val graph = DeBrujinGraph(qmerRdd)
    val contigRdd = graph.buildContigs()

    assert(contigRdd.count === 1)
    assert(contigRdd.first.fragment === "ACCCTGCGGCTCACATTATTTACAC")
  }

  sparkTest("reconstruct a messy tangle of reads") {

    val path = ClassLoader.getSystemClassLoader.getResource("NA12878_snp_A2G_chr20_225058.sam").getFile
    val rdd: RDD[ADAMRecord] = sc.adamLoad(path)

    val qmerRdd = ProcessReads(rdd, 30)

    val graph = DeBrujinGraph(qmerRdd)
    val contigRdd = graph.buildContigs()

    assert(contigRdd.count === 50)
  }

}
