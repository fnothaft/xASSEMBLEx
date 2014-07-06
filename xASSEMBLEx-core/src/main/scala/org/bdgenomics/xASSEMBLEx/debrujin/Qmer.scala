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
package org.bdgenomics.xASSEMBLEx.debrujin

import org.bdgenomics.xASSEMBLEx.util.NucleotideSequenceHash

/**
 * A quality score weighted k-mer. Stores _k_ bases of sequence.
 *
 * @param kmer K-base sequence.
 * @param support Quality weight for this k-mer.
 * @param next Edge to next k-mer.
 * @param nextSupport Quality score supporting the edge.
 */
case class Qmer(kmer: String,
                support: Double,
                next: Char,
                nextSupport: Double) extends Serializable {

  /**
   * Gets the ID of this q-mer, which is generated from the first 32 characters
   * of the q-mer's k-mer.
   *
   * @return Returns the q-mer ID.
   */
  def key(): Long = {
    NucleotideSequenceHash.hashSequence(kmer)
  }
}
