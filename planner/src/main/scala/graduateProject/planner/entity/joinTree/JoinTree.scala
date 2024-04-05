package graduateProject.planner.entity.joinTree

import graduateProject.planner.entity
import graduateProject.planner.entity.hypergraph.relationHypergraph.{Relation, Variable}

import scala.collection.mutable

class JoinTree(val nodeSet: Set[Relation], val edgeSet: Set[JoinTreeEdge]) {
  def isEmpty:Boolean=this.nodeSet.isEmpty

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
