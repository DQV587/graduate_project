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
  override def toString: String = "nodes:\r\n"+nodeSet.toString()+"\r\ncomparisons:\r\n"+edgeSet.toString()
  def degree:Int={
    nodeSet.map(node=>{
      edgeSet.count(edge => {
        edge.nodeSet.contains(node)
      })
    }).max
  }
  def isBergeAcyclic:Boolean={
    true
  }
  def getReducibleRelations():Set[Relation]={
    joinTree.getLeafs.filter(x=>isReducible(x))
  }
  def isReducible(relation:Relation):Boolean={
    if(relationComparisonsMap(relation).size<2) true
    else{
      val comparisonSet=relationComparisonsMap(relation)
      comparisonSet.count(comparison=>comparison.isLongComparison)<=1
    }
  }
  def reduceRelation(relation: Relation):ReduceInformation={
    assert(isReducible(relation))
    val reducedComparisonInformationSet=relationComparisonsMap(relation).map(x=>x.reduceIncidentRelation(relation)).toSet
    relationComparisonsMap(relation).foreach(comparison=>{
      if(comparison.isDone) {
        relationComparisonsMap(relation).remove(comparison)
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
