package graduateProject.planner.entity.hypergraph.relationHypergraph

import graduateProject.planner.entity.JoinTree.JoinTreeEdge
import graduateProject.planner.entity.hypergraph.{HyperGraph, HyperGraphEdge}
import graduateProject.planner.entity.query.Query

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class RelationHyperGraph(val nodeSet:Set[Variable],val edgeSet:Set[Relation]) extends HyperGraph[Variable,Relation] {
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
      val
    }
  }

  override def toString: String = {
    val builder=new mutable.StringBuilder()
    builder.append("nodes:\r\n")
    for(node<-getNodes){
      builder.append("\t").append(node.toString).append("\r\n")
    }
    builder.append("edges:\r\n")
    for(edge<-getEdges){
      builder.append("\t").append(edge.toString)
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
