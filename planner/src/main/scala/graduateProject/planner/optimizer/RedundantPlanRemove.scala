package graduateProject.planner.optimizer

import graduateProject.planner.entity.hypergraph.comparisonHypergraph.ReduceInformation

import scala.collection.mutable


object RedundantPlanRemove {
  def apply(plans:Set[List[ReduceInformation]]):List[List[ReduceInformation]]={
    val result=mutable.ArrayBuffer[List[ReduceInformation]]()
    plans.foreach(plan=>{
      if(result.forall(x=>(!x.last.relation.equals(plan.last.relation))))
        result.append(plan)
    })
    result.toList
  }
}
