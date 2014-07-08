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

import org.apache.spark.graphx.Edge
import org.bdgenomics.xASSEMBLEx.util.NucleotideSequenceHash
import scala.collection.mutable.HashMap

object MergedQmer {

  /**
   * Takes a collection of qmers, and merges them together into a single
   * qmer. The merged qmer aggregates the statistics for each qmer.
   *
   * @param iter An iterator of qmers to merge.
   * @return Returns a single qmer that summarizes the qmers at this position.
   */
  def apply(iter: Iterable[Qmer]): MergedQmer = {
    // get kmer
    val kmer = iter.head.kmer

    // number of qmers at this site
    var multiplicity = 0

    // support value
    var support = 0.0

    // create count and support arrays for next qmer
    val nextCount = Array(0, 0, 0, 0)
    val nextSupport = Array(0.0, 0.0, 0.0, 0.0)

    // iterate over qmers that we are merging
    iter.foreach(qmer => {
      // qmer must have the correct prefix
      assert(qmer.kmer == kmer, "Qmer did not match prefix of other qmers in merge.")

      // update multiplicity and support
      multiplicity += 1
      support += qmer.support

      // get index for next
      val idx = qmer.next match {
        case 'A'       => 0
        case 'C'       => 1
        case 'G'       => 2
        case 'T' | 'U' => 3
        case _         => throw new IllegalArgumentException("Illegal character.")
      }

      // update stat arrays
      nextCount(idx) += 1
      nextSupport(idx) += qmer.nextSupport
    })

    // generate statistics for next qmer
    val nextStats = Array(('A', nextCount(0), nextSupport(0)),
      ('C', nextCount(1), nextSupport(1)),
      ('G', nextCount(2), nextSupport(2)),
      ('T', nextCount(3), nextSupport(3)))
      .filter(t => t._2 != 0)

    // generate merged qmer
    MergedQmer(kmer,
      multiplicity,
      support,
      nextStats.map(t => t._1),
      nextStats.map(t => t._2),
      nextStats.map(t => t._3))
  }

  /**
   * Creates a new, previously untouched qmer.
   *
   * @param kmer K-base sequence.
   * @param multiplicity The number of q-mers with this sequence.
   * @param support Quality weight for this k-mer.
   * @param next Edge to next k-mer.
   * @param nextCount The number of links between stages.
   * @param nextSupport Quality score supporting the edge.
   */
  def apply(kmer: String,
            multiplicity: Int,
            support: Double,
            next: Array[Char],
            nextCount: Array[Int],
            nextSupport: Array[Double]): MergedQmer = {
    val id = NucleotideSequenceHash.hashSequence(kmer)
    new MergedQmer(id,
      kmer,
      multiplicity,
      support,
      next,
      nextCount,
      nextSupport,
      id,
      support,
      0,
      None.asInstanceOf[Option[Long]],
      None.asInstanceOf[Option[Long]],
      Map())
  }

  def update(qmer: MergedQmer,
             contigId: Long,
             contigScore: Double,
             contigRank: Int): MergedQmer = {
    new MergedQmer(qmer.id,
      qmer.kmer,
      qmer.multiplicity,
      qmer.support,
      qmer.next,
      qmer.nextCount,
      qmer.nextSupport,
      contigId,
      contigScore,
      contigRank,
      qmer.receiveIn,
      qmer.receiveOut,
      qmer.adjacentContigs)
  }

  def update(qmer: MergedQmer,
             receive: Long,
             in: Boolean): MergedQmer = {
    val (receiveIn, receiveOut) = if (in) {
      (Some(receive), qmer.receiveOut)
    } else {
      (qmer.receiveIn, Some(receive))
    }

    new MergedQmer(qmer.id,
      qmer.kmer,
      qmer.multiplicity,
      qmer.support,
      qmer.next,
      qmer.nextCount,
      qmer.nextSupport,
      qmer.contigId,
      qmer.contigScore,
      qmer.contigRank,
      receiveIn,
      receiveOut,
      qmer.adjacentContigs)
  }

  def update(qmer: MergedQmer,
             adjacentContigs: Map[Long, Long]): MergedQmer = {
    new MergedQmer(qmer.id,
      qmer.kmer,
      qmer.multiplicity,
      qmer.support,
      qmer.next,
      qmer.nextCount,
      qmer.nextSupport,
      qmer.contigId,
      qmer.contigScore,
      qmer.contigRank,
      qmer.receiveIn,
      qmer.receiveOut,
      adjacentContigs)
  }

}

/**
 * A quality score weighted k-mer. Stores _k_ bases of sequence.
 *
 * @param kmer K-base sequence.
 * @param support Quality weight for this k-mer.
 * @param next Edge to next k-mer.
 * @param nextSupport Quality score supporting the edge.
 */
case class MergedQmer(id: Long,
                      kmer: String,
                      multiplicity: Int,
                      support: Double,
                      next: Array[Char],
                      nextCount: Array[Int],
                      nextSupport: Array[Double],
                      contigId: Long,
                      contigScore: Double,
                      contigRank: Int,
                      receiveIn: Option[Long],
                      receiveOut: Option[Long],
                      adjacentContigs: Map[Long, Long]) {

  /**
   * Returns the ID of this qmer, it's kmer, and it's connections.
   *
   * @return A string describing the qmer.
   */
  override def toString(): String = {
    id + ": " + kmer + next.foldLeft("[")(_ + _) + "] on " + contigId
  }

  /**
   * Updates the adjacent contig map with a message from a sender.
   *
   * @param id Contig ID.
   * @param sender Sender ID.
   */
  def updateAdjacentContig(id: Long, sender: Long): MergedQmer = {
    MergedQmer.update(this, adjacentContigs.filter(kv => kv._1 != sender) + (sender -> id))
  }

  /**
   * Returns the adjacent contigs that we can connect to.
   *
   * @return Returns a list of contig IDs.
   */
  def getAdjacentContigs(): Iterable[Long] = {
    adjacentContigs.values
  }

  /**
   * Adds a message sending node to the set of acceptable senders.
   * Performs this for an in edge.
   *
   * @param id ID of the sending node.
   */
  def acceptInMessage(id: Long): MergedQmer = {
    MergedQmer.update(this, id, true)
  }

  /**
   * Adds a message sending node to the set of acceptable senders.
   * Performs this for an out edge.
   *
   * @param id ID of the sending node.
   */
  def acceptOutMessage(id: Long): MergedQmer = {
    MergedQmer.update(this, id, false)
  }

  /**
   * Returns true if we can accept a message from another node.
   *
   * @param id ID to check.
   * @return True if the message can be accepted.
   */
  def canAcceptMessageFrom(id: Long): Boolean = {
    receiveIn.fold(false)(_ == id) || receiveOut.fold(false)(_ == id)
  }

  /**
   * Sets the contig ID and score of this qmer.
   *
   * @param id Hash ID to set.
   * @param score Score to set.
   */
  def setContig(id: Long,
                score: Double,
                rank: Int): MergedQmer = {
    MergedQmer.update(this, id, score, rank)
  }

  /**
   * Gets the contig ID and score of this qmer.
   *
   * @return (ID, score, rank) for the contig this qmer belongs to.
   *
   * @throws AssertionError Throws an assertion error if this qmer does not yet
   * belong to a contig.
   */
  def getContig(): (Long, Double, Int) = {
    (contigId, contigScore, contigRank)
  }

  /**
   * Gets the rank of this qmer in it's respective contig.
   *
   * @return Returns the rank of this qmer in it's respective contig.
   */
  def getRank(): Int = contigRank

  /**
   * Gets the ID of this q-mer, which is generated from the first 32 characters
   * of the q-mer's k-mer.
   *
   * @return Returns the q-mer ID.
   */
  def key(): Long = id

  /**
   * Creates the edges connecting this qmer and all qmers near it that are
   * supported by reads.
   *
   * @return Returns an array of graph edges.
   */
  def emitEdges(): Array[Edge[QmerAdjacency]] = {

    // get the start of the next qmers
    val nextQmerPrefix = kmer.drop(1)

    // get the current id
    val srcId = id

    // create an array to fill in
    val array = new Array[Edge[QmerAdjacency]](next.length)

    // loop and build edges
    (0 until next.length).foreach(i => {
      // get id
      val destId = NucleotideSequenceHash.hashSequence(nextQmerPrefix + next(i))

      // build a new edge
      array(i) = Edge(srcId, destId, QmerAdjacency(nextCount(i), true))
    })

    // return filled array
    array
  }
}
