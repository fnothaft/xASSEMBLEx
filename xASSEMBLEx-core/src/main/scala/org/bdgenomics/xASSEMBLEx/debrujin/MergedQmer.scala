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
    new MergedQmer(kmer,
      multiplicity,
      support,
      nextStats.map(t => t._1),
      nextStats.map(t => t._2),
      nextStats.map(t => t._3))
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
class MergedQmer(val kmer: String,
                 val multiplicity: Int,
                 val support: Double,
                 val next: Array[Char],
                 val nextCount: Array[Int],
                 val nextSupport: Array[Double]) extends Serializable {

  // the id of the contig this is in, if it is in any contig
  var contigId: Option[Long] = None
  var contigScore: Option[Double] = None

  // the rank of this qmer in it's respective contig
  var contigRank = 0

  // the id of this qmer
  lazy val id = generateKey()

  // the list of IDs we can receive messages from
  var receiveIn: Option[Long] = None
  var receiveOut: Option[Long] = None

  // have we been updated this turn?
  var updated = false

  // map of contigs we can connect to
  val adjacentContigs = HashMap[Long, Long]()

  /**
   * Updates the adjacent contig map with a message from a sender.
   *
   * @param id Contig ID.
   * @param sender Sender ID.
   */
  def updateAdjacentContig(id: Long, sender: Long) {
    adjacentContigs(sender) = id
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
   * Has this qmer been added to a contig?
   */
  def partOfAContig: Boolean = contigId.isDefined

  /**
   * Adds a message sending node to the set of acceptable senders.
   * Performs this for an in edge.
   *
   * @param id ID of the sending node.
   */
  def acceptInMessage(id: Long) {
    receiveIn = Some(id)
  }

  /**
   * Adds a message sending node to the set of acceptable senders.
   * Performs this for an out edge.
   *
   * @param id ID of the sending node.
   */
  def acceptOutMessage(id: Long) {
    receiveOut = Some(id)
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
   *
   * @note This function sets the updated flag.
   */
  def setContig(id: Long,
                score: Double,
                rank: Int) {
    updated = true
    contigId = Some(id)
    contigScore = Some(score)
    contigRank = rank
  }

  /**
   * Checks to see if this qmer has been updated. Clears the updated flag
   * after checking.
   *
   * @return Returns true if this qmer has been updated.
   */
  def wasUpdated(): Boolean = {
    // get flag
    val wasUpdated = updated

    // clear flag
    updated = false

    // return value before clearing
    wasUpdated
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
    assert(contigId.isDefined, "Cannot get contig until qmer joins a contig")
    (contigId.get, contigScore.get, contigRank)
  }

  /**
   * Gets the rank of this qmer in it's respective contig.
   *
   * @return Returns the rank of this qmer in it's respective contig.
   */
  def getRank(): Int = contigRank

  /**
   * Generates the ID of this q-mer, which is generated from the first 32 characters
   * of the q-mer's k-mer.
   *
   * @return Returns the q-mer ID.
   */
  protected def generateKey(): Long = {
    NucleotideSequenceHash.hashSequence(kmer)
  }

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
      array(i) = Edge(srcId, destId, QmerAdjacency(nextCount(i)))
    })

    // return filled array
    array
  }
}
