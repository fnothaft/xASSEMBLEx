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

import org.bdgenomics.formats.avro.ADAMRecord
import org.scalatest.FunSuite

class ProcessReadsSuite extends FunSuite {

  test("cut a read into qmers") {
    val read = ADAMRecord.newBuilder()
      .setSequence("ACTGC")
      .setQual(".....")
      .build()

    val pr = new ProcessReads
    val qmers = pr.readToQmers(read, 4)

    assert(qmers.hasNext)
    val qmer = qmers.next
    assert(qmer.kmer === "ACTG")
    assert(qmer.next === 'C')
    assert(!qmers.hasNext)
  }

}
