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

import scala.annotation.tailrec

object NucleotideSequenceHash {

  /**
   * Hashes a sequence of nucleotides into a 64 bit long.
   *
   * @param sequence Nucleotide sequence to hash.
   * @return Returns a long hash.
   *
   * @throws IllegalArgumentException Throws an exception if the string contains
   * non-nucleotide (ACTUG) letters.
   */
  def hashSequence(sequence: String): Long = {
    @tailrec def hashHelper(iter: Iterator[Char], hash: Long): Long = {
      if (!iter.hasNext) {
        hash
      } else {
        val newHash = 4L * hash + iter.next match {
          case 'A'       => 0L
          case 'C'       => 1L
          case 'G'       => 2L
          case 'T' | 'U' => 3L
          case _         => throw new IllegalArgumentException("Saw non-nucleotide letter.")
        }
        hashHelper(iter, newHash)
      }
    }

    // we store a hash at 2 bits per nucleotide in a 64 bit long
    val seq = if (sequence.length > 32) {
      sequence.take(31)
    } else {
      sequence
    }

    // call tail recursive hash computing function
    hashHelper(sequence.toIterator, 0L)
  }

}
