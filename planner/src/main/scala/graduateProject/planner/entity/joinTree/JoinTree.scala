package graduateProject.planner.entity.joinTree

import graduateProject.planner.entity
import graduateProject.planner.entity.hypergraph.relationHypergraph.{Relation, Variable}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class JoinTree(var nodeSet: Set[Relation], var edgeSet: Set[JoinTreeEdge]) {
  def isEmpty:Boolean=this.nodeSet.isEmpty

  def getLeafs:Set[Relation]={
    val buffer=new ArrayBuffer[Relation]()
    for(relation<-nodeSet){
      var flag=true
      for(edge<-edgeSet){
        if(edge.father.equals(relation)){
          flag=false
        }
      }
      if(flag){
        buffer.append(relation)
      }
    }
    buffer.toSet
  }
  def isLeaf(input:Relation):Boolean={
    if(!nodeSet.contains(input)) false
    else{
      for(edge<-edgeSet){
        if(edge.father.equals(input)) return false
      }
      true
    }
  }
  def removeLeaf(leaf:Relation):Unit={
    if(isLeaf(leaf)){
      this.nodeSet=this.nodeSet-leaf
      for(edge<-edgeSet if edge.son.equals(leaf)){
        this.edgeSet=this.edgeSet-edge
      }
    }
  }
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
      builder.append(edge.son.getRelationId()).append("->").append(edge.father.getRelationId()).append(":").append(edge.sharedVariable).append("\r\n")
    }
    builder.toString()
  }
}
object JoinTree{

  def emptyJoinTree:JoinTree={
    new JoinTree(Set[Relation](),Set[JoinTreeEdge]())
  }
  def newJoinTree(oldTree:JoinTree,newEdge:JoinTreeEdge):JoinTree={
      new JoinTree(oldTree.nodeSet++Set(newEdge.son,newEdge.father),oldTree.edgeSet+newEdge)
  }
}


case class JoinTreeEdge(father:Relation,son:Relation,sharedVariable:Set[Variable]){
  override def toString: String = {
    "Father:\r\n"+father.toString+"\r\nSon:\r\n"+son.toString+"\r\n"
  }
}
