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

import org.apache.spark.Logging
import org.apache.spark.graphx.{
  Edge,
  EdgeDirection,
  EdgeTriplet,
  Graph,
  Pregel,
  VertexId
}
import org.apache.spark.rdd.RDD
import org.bdgenomics.xASSEMBLEx.contig.{ ContigBuilder, IntermediateContig }
import scala.annotation.tailrec
import scala.math.abs

object DeBrujinGraph {

  /**
   * Creates a de Brujin graph from the q-mers in the read dataset.
   *
   * @param rdd An RDD containing all the qmers in the graph.
   * @return Returns a constructed de Brujin graph.
   */
  def apply(rdd: RDD[MergedQmer]): DeBrujinGraph = {
    // cache rdd
    rdd.cache()

    // get edges and vertices
    val vertices = rdd.keyBy(_.key)
    val edges = rdd.flatMap(_.emitEdges())

    // unpersist rdd
    rdd.unpersist()

    // build graph, and filter out edges without nodes on both ends
    new DeBrujinGraph(Graph(vertices, edges)
      .subgraph(et => et.srcAttr != null && et.dstAttr != null))
  }
}

/**
 * DeBrujin graph; stores links between q-mers. Used for assembing q-mers into
 * contigs. We do this by:
 *
 * - Using the Pregel API in GraphX to label all nodes as part of a contig.
 * - Grouping all labeled qmers into intermediate contig fragments.
 *
 * @param graph A graph of qmers with connectivity.
 */
class DeBrujinGraph(graph: Graph[MergedQmer, QmerAdjacency]) extends Serializable with Logging {

  // have we labeled the nodes in this graph?
  lazy val labeledGraph = labelQmers()

  /**
   * Label all q-mers with the ID of the contig that they belong to.
   *
   * @return Returns a labeled graph.
   */
  protected def labelQmers(): Graph[MergedQmer, QmerAdjacency] = {

    /**
     * Message passed when labeling nodes with q-mer membership.
     *
     * @param id ID of the contig.
     * @param score Score of the contig.
     * @param messageSender ID of the sender of this message.
     *
     * @note Input parameters are stored as a list to enable message merging.
     */
    case class LabelingMessage(id: List[Long],
                               score: List[Double],
                               messageSender: List[Long],
                               rank: List[Int]) extends Serializable {
      assert(id.length == score.length && id.length == messageSender.length,
        "Input lists must all be the same length")

      /**
       * Merges two messages together.
       *
       * @param msg Message to merge.
       * @return Returns a new message.
       */
      def merge(msg: LabelingMessage): LabelingMessage = {
        LabelingMessage(id ::: msg.id,
          score ::: msg.score,
          messageSender ::: msg.messageSender,
          rank ::: msg.rank)
      }
    }

    object AdjacencyMessage {
      /**
       * Creates an empty adjacency message.
       *
       * @return Returns an empty adjacency message.
       */
      def apply(): AdjacencyMessage = {
        new AdjacencyMessage(None.asInstanceOf[Option[Long]])
      }

      /**
       * Creates a populated adjacency message.
       *
       * @param id ID to populate the message with.
       * @return Returns a populated message.
       */
      def apply(id: Long): AdjacencyMessage = {
        new AdjacencyMessage(Some(id))
      }
    }

    /**
     * Message passed when labeling nodes that are OK to receive messages from.
     *
     * @param id ID of the message sender.
     */
    case class AdjacencyMessage(id: Option[Long]) {

      /**
       * Merges two messages together. Merging another message kills both messages.
       *
       * @param msg Message to merge.
       * @return Returns a new message.
       */
      def merge(msg: AdjacencyMessage): AdjacencyMessage = {
        AdjacencyMessage(None)
      }

      override def toString(): String = id.toString
    }

    /**
     * Updates a q-mer with a received message. Changes the contig ID of the q-mer
     * if there is an acceptable message with a higher contig score.
     *
     * @param vid The ID of the q-mer.
     * @param qmer The q-mer to update.
     * @param msg The message to update.
     * @return Returns this qmer, possibly with a new contig ID.
     */
    def updateQmers(vid: VertexId,
                    qmer: MergedQmer,
                    msg: LabelingMessage): MergedQmer = {
      // get the score of this qmer's current contig
      val (cScore, cId) = if (qmer != null) {
        (qmer.getContig._2, qmer.getContig._1)
      } else {
        (0.0, 0L)
      }

      @tailrec def updateIfAccepted(id: Iterator[Long],
                                    score: Iterator[Double],
                                    sender: Iterator[Long],
                                    rank: Iterator[Int],
                                    qmer: MergedQmer): MergedQmer = {
        // do we have more data?
        if (!id.hasNext) {
          qmer
        } else {
          val nId = id.next
          val nScore = score.next
          val nSender = sender.next
          val nRank = rank.next

          val msg = "qmer " + vid + "/" + cScore + " in " + cId + " received (" + nId + "," + nScore + ") from " + nSender

          // if we can accept a message from this sender, and it has a better score
          // than our current score, we update
          val nQmer = qmer.setContig(nId, nScore, nRank)

          // call recursively
          updateIfAccepted(id, score, sender, rank, nQmer)
        }
      }

      // call our update function
      updateIfAccepted(msg.id.toIterator,
        msg.score.toIterator,
        msg.messageSender.toIterator,
        msg.rank.toIterator,
        qmer)
    }

    /**
     * Sends messages within a triplet. Messages are sent if either node's contig
     * ID was updated in the last iteration.
     *
     * @param et Edge triplet to send messages on.
     * @return Returns an iterator containing the IDs of the vertices to send
     * messages to, as well as the messages.
     */
    def sendMessage(et: EdgeTriplet[MergedQmer, QmerAdjacency]): Iterator[(VertexId, LabelingMessage)] = {
      def fpEquals(a: Double, b: Double): Boolean = {
        abs(a - b) < 1e-6
      }

      // if we can't send messages to each other, let's not
      if (et.attr.sendOnEdge) {
        return Iterator()
      }

      // if we have a long repeat, a qmer may try to send to itself repeatedly
      // this can trigger infinite messaging, which has a bad impact on runtime ;)
      // so, let's avoid that by returning early
      if (et.srcId == et.dstId) {
        log.warn("Qmer " + et.srcId + " is trying to send messages to itself.")
        return Iterator()
      }

      // get contig stats
      val srcContig = et.srcAttr.getContig
      val dstContig = et.dstAttr.getContig

      if (srcContig._1 == dstContig._1) {
        // don't send a message to yourself - see infinite messaging comment above
        return Iterator()
      }

      // which contig has a higher score?
      val (recepientVertexId,
        contigId,
        contigScore,
        senderId,
        recipientRank) = if (fpEquals(srcContig._2, dstContig._2)) {
        if (srcContig._1 > dstContig._1) {
          (et.dstId, srcContig._1, srcContig._2, et.srcId, srcContig._3 + 1)
        } else {
          (et.srcId, dstContig._1, dstContig._2, et.dstId, dstContig._3 - 1)
        }
      } else if (srcContig._2 > dstContig._2) {
        (et.dstId, srcContig._1, srcContig._2, et.srcId, srcContig._3 + 1)
      } else {
        (et.srcId, dstContig._1, dstContig._2, et.dstId, dstContig._3 - 1)
      }

      // convert to iterator and return
      Iterator((recepientVertexId, LabelingMessage(List(contigId),
        List(contigScore),
        List(senderId),
        List(recipientRank))))
    }

    // run two iterations of pregel to find allowable id's
    val inGraph = Pregel[MergedQmer, QmerAdjacency, AdjacencyMessage](graph, AdjacencyMessage(), 1, EdgeDirection.In)(
      (vid: VertexId, qmer: MergedQmer, msg: AdjacencyMessage) => {
        msg.id.foldLeft(qmer)(_.acceptInMessage(_))
      }, (et: EdgeTriplet[MergedQmer, QmerAdjacency]) => {
        Iterator((et.dstId, AdjacencyMessage(et.srcId)))
      }, _.merge(_))
    val outGraph = Pregel[MergedQmer, QmerAdjacency, AdjacencyMessage](inGraph, AdjacencyMessage(), 1, EdgeDirection.Out)(
      (vid: VertexId, qmer: MergedQmer, msg: AdjacencyMessage) => {
        msg.id.foldLeft(qmer)(_.acceptOutMessage(_))
      }, (et: EdgeTriplet[MergedQmer, QmerAdjacency]) => {
        Iterator((et.srcId, AdjacencyMessage(et.dstId)))
      }, _.merge(_))

    // map over edge triplets
    val finalGraph = outGraph.mapTriplets((_, iter) => iter.map(et => {
      if (et.dstAttr != null && et.dstAttr.receiveIn.isDefined) {
        QmerAdjacency(et.attr.readsCovering,
          true)
      } else {
        QmerAdjacency(et.attr.readsCovering,
          false)
      }
    }))

    // pregel the graph up for some old school contiggin' action
    Pregel[MergedQmer, QmerAdjacency, LabelingMessage](finalGraph, LabelingMessage(List(),
      List(),
      List(),
      List()))(
      updateQmers, sendMessage, _.merge(_))
  }

  /**
   * Connects labeled q-mers into contigs. If contigs have not yet been labeled,
   * labels the q-mers.
   *
   * @return Returns an RDD of contigs.
   *
   * @see org.bdgenomics.xASSEMBLEx.contig.ContigBuilder
   */
  def buildContigs(): RDD[IntermediateContig] = {
    labeledGraph.vertices
      .flatMap(kv => Option(kv._2))
      .groupBy(_.getContig._1)
      .map(kv => ContigBuilder(kv._1, kv._2))
  }
}
