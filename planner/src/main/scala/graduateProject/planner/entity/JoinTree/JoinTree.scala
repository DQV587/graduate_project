package graduateProject.planner.entity.JoinTree

import graduateProject.planner.entity
import graduateProject.planner.entity.hypergraph.relationHypergraph.{Relation, Variable}

class JoinTree(val nodeSet: Set[Relation], val edgeSet: Set[JoinTreeEdge]) {

}
object JoinTree{

  def emptyJoinTree:JoinTree={
    new JoinTree(Set[Relation](),Set[JoinTreeEdge]())
  }

  def newJoinTree(oldTree:JoinTree,newEdge:JoinTreeEdge):JoinTree={
    new JoinTree(oldTree.nodeSet+newEdge.father,oldTree.edgeSet+newEdge)
  }
}


case class JoinTreeEdge(father:Relation,son:Relation,sharedVariable:Set[Variable])
