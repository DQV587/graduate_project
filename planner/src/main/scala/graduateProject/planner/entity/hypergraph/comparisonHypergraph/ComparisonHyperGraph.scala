package graduateProject.planner.entity.hypergraph.comparisonHypergraph

import graduateProject.planner.entity.expression.Comparison
import graduateProject.planner.entity.hypergraph.HyperGraph
import graduateProject.planner.entity.hypergraph.relationHypergraph.Relation
import graduateProject.planner.entity.joinTree.{JoinTree, JoinTreeEdge}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ComparisonHyperGraph( val joinTree:JoinTree,edges:Set[ComparisonHyperGraphEdge],nodes:Set[JoinTreeEdge]) extends HyperGraph[JoinTreeEdge,ComparisonHyperGraphEdge]{

  override var edgeSet: Set[ComparisonHyperGraphEdge] = edges
  override var nodeSet: Set[JoinTreeEdge] = nodes
  val relationComparisonsMap: mutable.Map[Relation, mutable.Set[ComparisonHyperGraphEdge]] = {
    val map = mutable.Map[Relation, mutable.Set[ComparisonHyperGraphEdge]]()
    joinTree.nodeSet.map(relation=>map.put(relation, mutable.Set[ComparisonHyperGraphEdge]()))
    edges.filter(edge=>(edge.leftRelation.nonEmpty && edge.rightRelation.nonEmpty)).map(edge=>{
      map(edge.leftRelation.get).add(edge)
      map(edge.rightRelation.get).add(edge)
    })
    map
  }
  override def toString: String = nodeSet.toString()+"\r\n"+edgeSet.toString()
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
  def reduce(relation: Relation):Unit={
    assert(isReducible(relation))

  }
}
