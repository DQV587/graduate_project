package graduateProject.planner.planGenerator

case class PhysicalPlan(beforeActions:List[BeforeAction],cqcActions: List[CqcAction],afterActions:List[AfterAction]){
  def getCost():Int={
    1
  }
}
