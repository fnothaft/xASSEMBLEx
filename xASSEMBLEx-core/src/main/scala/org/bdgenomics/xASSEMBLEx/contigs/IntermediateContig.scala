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

/**
 * Intermediate representation of a contig before resolving splits,
 * repeats, or scaffolds.
 *
 * @param id Contig ID.
 * @param fragment The bases in this fragment.
 * @param headAdjacentContigs A list of the contigs that abut this contig.
 * @param tailAdjacentContigs A list of the contigs that abut this contig.
 */
case class IntermediateContig(id: Long,
                              fragment: String,
                              headAdjacentContigs: Iterable[Long],
                              tailAdjacentContigs: Iterable[Long]) {

  def toDot(): String = {
    id + " [\nlabel = \"" + fragment + "\"\nshape = box\n];\n" + headAdjacentContigs.map(l => {
      id + " -> " + l + ";\n"
    }).fold("")(_ + _)
  }
}

