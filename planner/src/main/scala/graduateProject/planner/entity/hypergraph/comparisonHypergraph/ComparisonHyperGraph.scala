package graduateProject.planner.entity.hypergraph.comparisonHypergraph

import graduateProject.planner.entity.expression.Comparison
import graduateProject.planner.entity.hypergraph.HyperGraph
import graduateProject.planner.entity.hypergraph.relationHypergraph.Relation
import graduateProject.planner.entity.joinTree.{JoinTree, JoinTreeEdge}

import scala.collection.mutable

class ComparisonHyperGraph( val joinTree:JoinTree,edges:Set[ComparisonHyperGraphEdge],nodes:Set[JoinTreeEdge]) extends HyperGraph[JoinTreeEdge,ComparisonHyperGraphEdge]{

  override var edgeSet: Set[ComparisonHyperGraphEdge] = edges
  override var nodeSet: Set[JoinTreeEdge] = nodes
  val relationComparisonsMap: mutable.Map[Relation, mutable.Set[ComparisonHyperGraphEdge]] = {
    val map = mutable.Map[Relation, mutable.Set[ComparisonHyperGraphEdge]]()
    joinTree.nodeSet.foreach(relation=>map.put(relation, mutable.Set[ComparisonHyperGraphEdge]()))
    edges.filter(edge=>(edge.leftRelation.nonEmpty && edge.rightRelation.nonEmpty)).foreach(edge=>{
      map(edge.leftRelation.get).add(edge)
      map(edge.rightRelation.get).add(edge)
    })
    map
  }

  override def nonEmpty: Boolean = nodeSet.nonEmpty

  override def isEmpty: Boolean = nodeSet.isEmpty
  override def toString: String = "nodes:\r\n"+nodeSet.toString()+"\r\ncomparisons:\r\n"+edgeSet.toString()
  def degree:Int={
    nodeSet.map(node=>{
      edgeSet.count(edge => {
        edge.nodeSet.contains(node)
      })
    }).max
  }
  def isBergeAcyclic:Boolean={
    // create a bipartite graph with the comparisons as one set and the joinTreeEdges as another set.
    // one element from the joinTreeEdges set is connected with one element from the comparisons set
    // if and only if the comparison contains the joinTreeEdge
    if(this.edgeSet.isEmpty) return true
    val nodesInBipartiteGraph: Set[Either[ComparisonHyperGraphEdge, JoinTreeEdge]] = edges.map(cmp => Left(cmp)) ++
      nodes.map(joinTreeEdge => Right(joinTreeEdge))
    val nodesInBipartiteGraphToId: Map[Either[ComparisonHyperGraphEdge, JoinTreeEdge], Int] = nodesInBipartiteGraph.zipWithIndex.toMap
    val bipartiteGraphEdges: mutable.HashMap[Int, mutable.HashSet[Int]] = mutable.HashMap.empty
    edges.foreach(cmp => {
      val cmpId = nodesInBipartiteGraphToId(Left(cmp))
      val joinTreeEdges = cmp.getNodes
      joinTreeEdges.foreach(joinTreeEdge => {
        val joinTreeEdgeId = nodesInBipartiteGraphToId(Right(joinTreeEdge))
        bipartiteGraphEdges.getOrElseUpdate(cmpId, mutable.HashSet.empty).add(joinTreeEdgeId)
        bipartiteGraphEdges.getOrElseUpdate(joinTreeEdgeId, mutable.HashSet.empty).add(cmpId)
      })
    })

    // check if circle exists
    val visitedNodes: mutable.HashSet[Int] = mutable.HashSet.empty
    val predecessors: Array[Int] = Array.fill(nodesInBipartiteGraph.size)(-1)

    def noCircle(node: Int): Boolean = {
      if (!visitedNodes.contains(node)) {
        // next node can any connected node other than the predecessor
        val nextNodes = bipartiteGraphEdges(node).toSet - predecessors(node)
        if (nextNodes.intersect(visitedNodes).nonEmpty) {
          // this node connects with a visited node which differs from its predecessor
          false
        } else {
          visitedNodes.add(node)
          nextNodes.forall(next => {
            predecessors(next) = node
            noCircle(next)
          })
        }
      } else {
        true
      }
    }

    nodesInBipartiteGraphToId.values.forall(id => noCircle(id))
  }
  def getReducibleRelations:Set[Relation]={
    joinTree.getLeafs.filter(x=>isReducible(x))
  }
  def isReducible(relation:Relation):Boolean={
    if(relationComparisonsMap(relation).size<2) true
    else{
      val comparisonSet=relationComparisonsMap(relation)
      comparisonSet.count(comparison=>comparison.isLongComparison)<=1
    }
  }
  def copy:ComparisonHyperGraph={
    val newNodesSet=this.nodeSet
    val edgeList=mutable.ArrayBuffer[ComparisonHyperGraphEdge]()
    this.edgeSet.foreach(edge=>{edgeList.append(edge.copy)})
    val newEdgesSet=edgeList.toSet
    new ComparisonHyperGraph(this.joinTree.copy,newEdgesSet,newNodesSet)
  }
  def reduceRelation(relation: Relation):ReduceInformation={
    assert(isReducible(relation))
    val reducedComparisonInformationSet=relationComparisonsMap(relation).map(x=>x.reduceIncidentRelation(relation)).toSet
    relationComparisonsMap(relation).foreach(comparison=>{
      if(comparison.isDone) {
        relationComparisonsMap(relation).remove(comparison)
        relationComparisonsMap({
          if(comparison.leftRelation.get.equals(relation))
            comparison.rightRelation.get
          else
            comparison.leftRelation.get
        }).remove(comparison)
        this.edgeSet=edgeSet-comparison
      }
      else {
        if(comparison.reducedLeftRelation.get){
          relationComparisonsMap(comparison.leftRelation.get).add(comparison)
        }
        else relationComparisonsMap(comparison.rightRelation.get).add(comparison)
      }
    })
    relationComparisonsMap.remove(relation)
    val reducedJoinTreeEdge=joinTree.removeLeaf(relation)
    if(reducedJoinTreeEdge.nonEmpty)
      this.nodeSet=nodeSet-reducedJoinTreeEdge.get
    ReduceInformation(relation = relation, reduceComparisonInformation = reducedComparisonInformationSet, reducedJoinTreeEdge = reducedJoinTreeEdge)
  }
}

case class ReduceInformation(relation: Relation,
                             reduceComparisonInformation: Set[ReduceComparisonInformation],
                             reducedJoinTreeEdge:Option[JoinTreeEdge])
