package graduateProject.planner.entity.hypergraph.relationHypergraph

import graduateProject.planner.entity.joinTree.JoinTreeEdge
import graduateProject.planner.entity.hypergraph.{HyperGraph, HyperGraphEdge}
import graduateProject.planner.entity.query.Query

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class RelationHyperGraph(var nodeSet:Set[Variable],var edgeSet:Set[Relation]) extends HyperGraph[Variable,Relation] {
  def removeEdge(edge: Relation): RelationHyperGraph = {
    val oldNodeSet = this.getNodes
    val oldEdgeSet = this.getEdges
    if (oldEdgeSet.contains(edge)) {
      val newEdgeSet = oldEdgeSet - edge
      var newNodeSet = oldNodeSet
      for (vertex <- edge.getNodes) {
        var flag = false
        for (restEdge <- newEdgeSet) {
          if (restEdge.getNodes.contains(vertex)) {
            flag = true
          }
        }
        if (flag) {
          newNodeSet = newNodeSet - vertex
        }
      }
      new RelationHyperGraph(newNodeSet,newEdgeSet)
    }
    else
      this
  }

  def getEars:Set[JoinTreeEdge]={

    val result=new ArrayBuffer[JoinTreeEdge]()
    for(potentialEar<-this.edgeSet){
      val thisVariableSet=potentialEar.getVariables()
      for(potentialWitness<-this.edgeSet if !potentialEar.equals(potentialWitness)){
        val otherVariableSet=potentialWitness.getVariables()
        if((thisVariableSet&otherVariableSet).nonEmpty) {
          val restVariables = thisVariableSet &~ (thisVariableSet & otherVariableSet)
          var flag = true
          for (otherRelation <- this.edgeSet if !(potentialEar.equals(otherRelation) || potentialWitness.equals(otherRelation))) {
            if (restVariables.subsetOf(otherRelation.getVariables())) {
              flag = false
            }
          }
          if (flag) {
            result.append(JoinTreeEdge(potentialWitness, potentialEar, thisVariableSet & otherVariableSet))
          }
        }
      }
    }
    result.toSet
  }

  def isAcyclic:Boolean={
    var curGraph=this
    while(curGraph.getEdges.size>1){
      val ears=curGraph.getEars
      if(ears.nonEmpty){
        val ear=ears.head.relation2
        curGraph=curGraph.removeEdge(ear)
      }
      else return false
    }
    true
  }

  override def toString: String = {
    val builder=new mutable.StringBuilder()
    builder.append("nodes:\r\n")
    for(node<-getNodes){
      builder.append("\t").append(node.toString)
    }
    builder.append("edges:\r\n")
    for(edge<-getEdges){
      builder.append("\t").append(edge.toString).append("\r\n")
    }
    builder.append("\r\n")
    builder.toString()
  }

}

object RelationHyperGraph{
  def constructFromQuery(query: Query): RelationHyperGraph = {
    var variableSet=Set[Variable]()
    val verticesSet:Set[Variable]={
      for(relation<-query.relations){
        for(variable<-relation.getVariables()){
          if(!variableSet.contains(variable)){
            variableSet=variableSet+variable
          }
        }
      }
      variableSet
    }
    new RelationHyperGraph(verticesSet, query.relations.toSet)
  }
}
