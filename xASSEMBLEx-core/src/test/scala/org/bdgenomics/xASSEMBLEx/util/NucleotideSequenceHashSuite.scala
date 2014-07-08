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
package org.bdgenomics.xASSEMBLEx.util

import org.scalatest.FunSuite

class NucleotideSequenceHashSuite extends FunSuite {

  test("can't hash non-nucleotide sequences") {
    intercept[IllegalArgumentException] {
      NucleotideSequenceHash.hashChar('Q')
    }
    intercept[IllegalArgumentException] {
      NucleotideSequenceHash.hashSequence("ACTZG")
    }
  }

  test("hash basic chars") {
    assert(NucleotideSequenceHash.hashChar('A') === 0L)
    assert(NucleotideSequenceHash.hashChar('C') === 1L)
    assert(NucleotideSequenceHash.hashChar('G') === 2L)
    assert(NucleotideSequenceHash.hashChar('T') === 3L)
    assert(NucleotideSequenceHash.hashChar('U') === 3L)
  }

  test("hash basic strings") {
    assert(NucleotideSequenceHash.hashSequence("ACGT") === (3L + 4L * 2L + 16L * 1L + 64L * 0L))
    assert(NucleotideSequenceHash.hashSequence("TGCA") === (0L + 4L * 1L + 16L * 2L + 64L * 3L))

    // two strings, when reversed, cannot have equal hashes
    assert(NucleotideSequenceHash.hashSequence("ACGT") != NucleotideSequenceHash.hashSequence("TGCA"))
  }

}
