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
      if(nodeSet.size==1) true
      else {
        if (edgeSet.count(edge => edge.isRelatedToRelation(input)) > 1) false
        else true
      }
    }
  }
  def removeLeaf(leaf:Relation):Option[JoinTreeEdge]={
    assert(isLeaf(leaf))
    this.nodeSet = nodeSet - leaf
    if(nodeSet.size>1) {
      val relatedJoinTreeEdge = this.edgeSet.filter(edge => edge.isRelatedToRelation(leaf)).head
      this.edgeSet = edgeSet - relatedJoinTreeEdge
      Some(relatedJoinTreeEdge)
    }
    else None
  }
//  override def clone(): JoinTree = super.clone()
  override def equals(obj: Any): Boolean = {
    if(!obj.isInstanceOf[JoinTree]) false
    else{
      val that=obj.asInstanceOf[JoinTree]
      if(!that.nodeSet.equals(this.nodeSet)) {
        nodeSet.forall(relation=>{
          val relatedEdgeInThis=this.edgeSet.filter(edge=>edge.isRelatedToRelation(relation))
          val relatedEdgeInThat=that.edgeSet.filter(edge=>edge.isRelatedToRelation(relation))
          if(relatedEdgeInThis.size!=relatedEdgeInThat.size) false
          else{
            relatedEdgeInThis.map(edge=>edge.getOtherRelation(relation)).equals(
              relatedEdgeInThat.map(edge=>edge.getOtherRelation(relation)))
          }
        })
      }
      else true
    }
  }
  override def toString: String = {
    val builder=new mutable.StringBuilder()
    edgeSet.foreach(edge=>
      builder.append(edge.toString).append(":").append(edge.sharedVariable).append("\r\n"))
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

}


case class JoinTreeEdge(relation1:Relation, relation2:Relation, sharedVariable:Set[Variable]){
  override def toString: String = {
    "(Relation1: "+relation1.getRelationId()+" Relation2: "+relation2.getRelationId()+")"
  }
  def isRelatedToRelation(relation: Relation):Boolean=relation1.equals(relation)||relation2.equals(relation)
  def getOtherRelation(relation: Relation):Relation={
    assert(isRelatedToRelation(relation))
    if(relation1.equals(relation)) relation2 else relation1
  }
}
