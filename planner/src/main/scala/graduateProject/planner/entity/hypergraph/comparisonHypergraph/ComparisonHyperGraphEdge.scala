package graduateProject.planner.entity.hypergraph.comparisonHypergraph

import graduateProject.planner.entity.expression.Comparison
import graduateProject.planner.entity.hypergraph.HyperGraphEdge
import graduateProject.planner.entity.hypergraph.relationHypergraph.Relation
import graduateProject.planner.entity.joinTree.JoinTreeEdge

import scala.collection.mutable

class ComparisonHyperGraphEdge(val comparison: Comparison,edges:Set[JoinTreeEdge],left:Option[Relation]=None,right: Option[Relation]=None) extends HyperGraphEdge[JoinTreeEdge]{
  var nodeSet: Set[JoinTreeEdge] =edges
  var leftRelation:Option[Relation]=left
  var rightRelation:Option[Relation]=right
  def isLongComparison:Boolean=this.edges.size>1
  def isShortComparison:Boolean=this.edges.size==1
  var isDone=false
  var reducedLeftRelation:Option[Boolean]=None
  //the relation to reduced is a leaf of the join tree, so there is only one edge whose son is exactly the relation.
  override def toString: String = {
    val builder=new mutable.StringBuilder()
    builder.append("left relation: ").append(leftRelation.get).append("\r\n")
    builder.append("right relation: ").append(rightRelation.get).append("\r\n")
    builder.append(comparison.toString).append("\r\n")
    builder.append(nodeSet.toString()).append("\r\n")
    builder.toString()
  }
  def copy:ComparisonHyperGraphEdge={
    new ComparisonHyperGraphEdge(comparison, edges, left, right)
  }
  def reduceIncidentRelation(relation: Relation):ReduceComparisonInformation={
    val isLeft:Boolean=this.leftRelation.get.equals(relation)
    val isLong=this.isLongComparison
    val reducedRelation=if(isLeft) leftRelation.get else rightRelation.get
    val joinTreeEdgeToReduced=nodeSet.filter(node=>node.isRelatedToRelation(reducedRelation)).head
    val newIncidentRelation=joinTreeEdgeToReduced.getOtherRelation(reducedRelation)
    this.nodeSet=nodeSet-joinTreeEdgeToReduced
    // one jointree edge can occur in many comparisons
    // for long comparisons the variable in the comparison is needed to preserve in the father relation
//    if(isLong) newIncidentRelation.addNodes(if(isLeft) comparison.left.getVariables else comparison.right.getVariables)
    //the short comparison need to be solved after reduce immediately,
    //the method can be filter for single comparison related case or C1 comparison for multiple comparisons case
    //or by range search method in which implicitly resolved
    if(isShortComparison) isDone=true
    if(isLeft) {
      leftRelation=Some(newIncidentRelation)
      this.reducedLeftRelation=Some(true)
    }
    else {
      rightRelation=Some(newIncidentRelation)
      this.reducedLeftRelation=Some(false)
    }
    ReduceComparisonInformation(isLong, isLeft, comparison)
  }
}
case class ReduceComparisonInformation(isLong:Boolean,isLeft:Boolean,comparison: Comparison)