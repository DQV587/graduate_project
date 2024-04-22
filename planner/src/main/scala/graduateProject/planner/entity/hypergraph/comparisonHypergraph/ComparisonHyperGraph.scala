package graduateProject.planner.entity.hypergraph.comparisonHypergraph

import graduateProject.planner.entity.expression.Comparison
import graduateProject.planner.entity.hypergraph.HyperGraph
import graduateProject.planner.entity.hypergraph.relationHypergraph.Relation
import graduateProject.planner.entity.joinTree.{JoinTree, JoinTreeEdge}

import scala.collection.mutable.ArrayBuffer

class ComparisonHyperGraph( val joinTree:JoinTree,edges:Set[ComparisonHyperGraphEdge],nodes:Set[JoinTreeEdge]) extends HyperGraph[JoinTreeEdge,ComparisonHyperGraphEdge]{

  override var edgeSet: Set[ComparisonHyperGraphEdge] = edges
  override var nodeSet: Set[JoinTreeEdge] = nodes

  override def toString: String = nodeSet.toString()+"\r\n"+edgeSet.toString()
  def degree:Int={
    2
  }
  def isBergeAcyclic:Boolean={
    true
  }

}
