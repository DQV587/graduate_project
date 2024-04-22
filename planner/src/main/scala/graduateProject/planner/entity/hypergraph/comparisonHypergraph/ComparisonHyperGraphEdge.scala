package graduateProject.planner.entity.hypergraph.comparisonHypergraph

import graduateProject.planner.entity.expression.Comparison
import graduateProject.planner.entity.hypergraph.HyperGraphEdge
import graduateProject.planner.entity.hypergraph.relationHypergraph.Relation
import graduateProject.planner.entity.joinTree.JoinTreeEdge

class ComparisonHyperGraphEdge(val comparison: Comparison,edges:Set[JoinTreeEdge],left:Option[Relation]=None,right: Option[Relation]=None) extends HyperGraphEdge[JoinTreeEdge]{
  var nodeSet: Set[JoinTreeEdge] =edges
  var leftRelation:Option[Relation]=left
  var rightRelation:Option[Relation]=right
  def isLongComparison:Boolean=this.edges.size>1

  //the relation to reduced is a leaf of the join tree, so there is only one edge whose son is exactly the relation.
  override def toString: String = {
    val builder=new StringBuilder()
    builder.append("left relation:\r\n").append(leftRelation.get).append("\r\n")
    builder.append("right relation:\r\n").append(rightRelation.get).append("\r\n")
    builder.append(comparison.toString).append("\r\n")
    builder.append(nodeSet.toString())
    builder.toString()
  }
  def reduceIncidentRelation(isLeft:Boolean):Unit={
    val reducedRelation=if(isLeft) leftRelation.get else rightRelation.get
    var newIncidentRelation=reducedRelation
    for(node<-nodeSet){
      if(node.son.equals(reducedRelation)){
        newIncidentRelation=node.father
        this.removeNode(node)
      }
    }
    newIncidentRelation.addNodes(if(isLeft) comparison.left.getVariables else comparison.right.getVariables)
    if(isLeft) leftRelation=Some(newIncidentRelation)
    else rightRelation=Some(newIncidentRelation)
  }
}
