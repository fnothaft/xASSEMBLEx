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

import org.apache.spark.graphx.{
  Edge,
  EdgeDirection,
  EdgeTriplet,
  Graph,
  Pregel,
  VertexId
}
import org.apache.spark.rdd.RDD
import scala.annotation.tailrec

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

    // build graph
    new DeBrujinGraph(Graph(vertices, edges))
  }
}

/**
 * DeBrujin graph; stores links between q-mers. Used for assembing q-mers into
 * contigs. We do this by:
 *
 * - Using the Pregel API in GraphX to label all nodes as part of a contig.
 *
 * @param graph 
 */
class DeBrujinGraph(graph: Graph[MergedQmer, QmerAdjacency]) extends Serializable {

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
                               messageSender: List[Long]) extends Serializable {
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
          messageSender ::: msg.messageSender)
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
      def merge(msg: AdjacencyMessage): AdjacencyMessage = AdjacencyMessage(None)
    }

    /**
     * Updates a q-mer with a received message. Changes the contig ID of the q-mer
     * if there is an acceptable message with a higher contig score.
     *
     * @param id The ID of the q-mer.
     * @param qmer The q-mer to update.
     * @param msg The message to update.
     * @return Returns this qmer, possibly with a new contig ID.
     */
    def updateQmers(id: VertexId,
                    qmer: MergedQmer,
                    msg: LabelingMessage): MergedQmer = {
      @tailrec def updateIfAccepted(id: Iterator[Long],
                                    score: Iterator[Double],
                                    sender: Iterator[Long],
                                    qmer: MergedQmer) {
        // do we have more data?
        if (id.hasNext) {
          val nId = id.next
          val nScore = score.next
          val nSender = sender.next

          // if we can accept a message from this sender, and it has a better score
          // than our current score, we update
          if (qmer.canAcceptMessageFrom(nSender) && qmer.getContig._2 < nScore) {
            qmer.setContig(nId, nScore)
          }

          // call recursively
          updateIfAccepted(id, score, sender, qmer)
        }
      }

      // call our update function
      updateIfAccepted(msg.id.toIterator,
        msg.score.toIterator,
        msg.messageSender.toIterator,
        qmer)

      // return our updated qmer
      qmer
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
      var msgList = List[(VertexId, LabelingMessage)]()

      // did the source get updated?
      if (et.srcAttr.wasUpdated()) {
        val contig = et.srcAttr.getContig
        msgList = (et.dstId, LabelingMessage(List(contig._1),
          List(contig._2),
          List(et.srcId))) :: msgList
      }

      // did the sink get updated?
      if (et.dstAttr.wasUpdated()) {
        val contig = et.dstAttr.getContig
        msgList = (et.srcId, LabelingMessage(List(contig._1),
          List(contig._2),
          List(et.dstId))) :: msgList
      }

      // convert to iterator and return
      msgList.toIterator
    }

    // run two iterations of pregel to find allowable id's
    val inGraph = Pregel[MergedQmer, QmerAdjacency, AdjacencyMessage](graph, AdjacencyMessage(None), 1, EdgeDirection.In)(
      (vid: VertexId, qmer: MergedQmer, msg: AdjacencyMessage) => {
        msg.id.foreach(qmer.acceptInMessage)
        qmer
      }, (et: EdgeTriplet[MergedQmer, QmerAdjacency]) => {
        Iterator((et.dstId, AdjacencyMessage(Some(et.srcId))))
      }, _.merge(_))
    val outGraph = Pregel[MergedQmer, QmerAdjacency, AdjacencyMessage](inGraph, AdjacencyMessage(None), 1, EdgeDirection.Out)(
      (vid: VertexId, qmer: MergedQmer, msg: AdjacencyMessage) => {
        msg.id.foreach(qmer.acceptInMessage)
        qmer
      }, (et: EdgeTriplet[MergedQmer, QmerAdjacency]) => {
        Iterator((et.srcId, AdjacencyMessage(Some(et.dstId))))
      }, _.merge(_))

    // pregel the graph up for some old school contiggin' action
    Pregel[MergedQmer, QmerAdjacency, LabelingMessage](outGraph, LabelingMessage(List(), List(), List()))(
      updateQmers, sendMessage, _.merge(_))
  }
}
