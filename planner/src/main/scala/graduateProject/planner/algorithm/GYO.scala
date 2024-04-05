package graduateProject.planner.algorithm

import graduateProject.planner.entity.hypergraph.HyperGraph
import graduateProject.planner.entity.hypergraph.relationHypergraph.RelationHyperGraph
import graduateProject.planner.entity.joinTree.JoinTree

import scala.collection.mutable

object GYO {
  def apply(hyperGraph: RelationHyperGraph):Set[JoinTree]={
    val initState=GyoState(hyperGraph,JoinTree.emptyJoinTree)
    var result=Set[JoinTree]()
    mutable.Stack
    var stateStack=List[GyoState](initState)
    while(stateStack.nonEmpty){
      var tmpStack=List[GyoState]()
      for(curState<-stateStack){
        if(curState.hyperGraph.getEdges.size>1){
          val ears=curState.hyperGraph.getEars
          for(ear<-ears){
            val newGraph=curState.hyperGraph.removeEdge(ear.son)
            val newJoinTree=JoinTree.newJoinTree(curState.joinTree,ear)
            tmpStack=tmpStack:+GyoState(newGraph,newJoinTree)
          }
        }
        else {
          result=result+curState.joinTree
        }
      }
      stateStack=tmpStack
    }
    result
  }
}

case class GyoState(hyperGraph: RelationHyperGraph,joinTree: JoinTree)
