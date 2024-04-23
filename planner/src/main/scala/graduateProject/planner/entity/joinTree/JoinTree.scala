package graduateProject.planner.entity.joinTree

import graduateProject.planner.entity
import graduateProject.planner.entity.hypergraph.relationHypergraph.{Relation, Variable}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class JoinTree(var nodeSet: Set[Relation], var edgeSet: Set[JoinTreeEdge]) {
  def isEmpty:Boolean=this.nodeSet.isEmpty

  def getLeafs:Set[Relation]={
    nodeSet.filter(relation=>{
      isLeaf(relation)
    })
  }
  def isLeaf(input:Relation):Boolean={
    if(!nodeSet.contains(input)) false
    else{
      if(edgeSet.count(edge=>edge.isRelatedToRelation(input))>1) false
      else true
    }
  }
  def removeLeaf(leaf:Relation):Unit={
    if(isLeaf(leaf)){
      this.nodeSet=this.nodeSet-leaf
      for(edge<-edgeSet if edge.isRelatedToRelation(leaf)){
        this.edgeSet=this.edgeSet-edge
      }
    }
  }
//  override def clone(): JoinTree = super.clone()
  override def equals(obj: Any): Boolean = {
    if(!obj.isInstanceOf[JoinTree]) false
    else{
      val that=obj.asInstanceOf[JoinTree]
      if((!that.nodeSet.equals(this.nodeSet))||(!that.edgeSet.equals(this.edgeSet))) false
      else true
    }
  }
  override def toString: String = {
    val builder=new mutable.StringBuilder()
    for(edge<-edgeSet){
      builder.append(edge.toString).append(":").append(edge.sharedVariable).append("\r\n")
    }
    builder.toString()
  }
}
object JoinTree{

  def emptyJoinTree:JoinTree={
    new JoinTree(Set[Relation](),Set[JoinTreeEdge]())
  }
  def newJoinTree(oldTree:JoinTree,newEdge:JoinTreeEdge):JoinTree={
      new JoinTree(oldTree.nodeSet++Set(newEdge.relation2,newEdge.relation1),oldTree.edgeSet+newEdge)
  }
//  def reduceIsomorphismJoinTrees(joinTreeSet:Set[JoinTree]):Set[JoinTree]={
//
//  }
}


case class JoinTreeEdge(relation1:Relation, relation2:Relation, sharedVariable:Set[Variable]){
  override def toString: String = {
    "Father: "+relation1.getRelationId()+" Son: "+relation2.getRelationId()
  }
  def isRelatedToRelation(relation: Relation):Boolean=relation1.equals(relation)||relation2.equals(relation)
}
