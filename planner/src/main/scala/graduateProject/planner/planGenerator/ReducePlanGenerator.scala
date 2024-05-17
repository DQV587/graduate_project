package graduateProject.planner.planGenerator

import graduateProject.planner.entity.hypergraph.comparisonHypergraph.{ComparisonHyperGraph, ReduceInformation}
import graduateProject.planner.entity.hypergraph.relationHypergraph.{AggregatedRelation, TableScanRelation}

import scala.collection.mutable

object ReducePlanGenerator {
  def apply(comparisonHyperGraph: ComparisonHyperGraph):Set[List[ReduceInformation]]={
    val reducePlanSet = mutable.Set[List[ReduceInformation]]()
    val stack=mutable.Stack[ReduceState]()
    stack.push(ReduceState(List[ReduceInformation](),comparisonHyperGraph))
    while(stack.nonEmpty){
      val curState=stack.pop()
      val reducibleRelationSet = curState.chg.getReducibleRelations
      if(reducibleRelationSet.isEmpty){
        reducePlanSet.add(curState.informationList)
      }
      else{
        if (reducibleRelationSet.forall(relation => relation.isInstanceOf[TableScanRelation])){
          reducibleRelationSet.foreach(relation=>{
            val newGraph=curState.chg.copy
            val newList=curState.informationList:+newGraph.reduceRelation(relation)
            stack.push(ReduceState(newList,newGraph))
          })
        }
        else{
          val nextReduceRelation =reducibleRelationSet.filter(relation => relation.isInstanceOf[AggregatedRelation]).head
          val information = curState.chg.reduceRelation(nextReduceRelation)
          stack.push(ReduceState(curState.informationList:+information,curState.chg))
        }
      }
    }
    reducePlanSet.toSet
    //    val reduceInformationArray = mutable.ArrayBuffer[ReduceInformation]()
    //    val nodeNum = comparisonHyperGraph.nodeSet.size
    //    for (i <- 0 to nodeNum) {
    //      val reducibleRelationSet = comparisonHyperGraph.getReducibleRelations
    //      val nextReduceRelation = {
    //        if (reducibleRelationSet.forall(relation => relation.isInstanceOf[TableScanRelation]))
    //          reducibleRelationSet.head
    //        else
    //          reducibleRelationSet.filter(relation => relation.isInstanceOf[AggregatedRelation]).head
    //      }
    //      val information = comparisonHyperGraph.reduceRelation(nextReduceRelation)
    //      reduceInformationArray.append(information)
    //    }
  }
}

case class ReduceState(informationList:List[ReduceInformation],chg:ComparisonHyperGraph)

