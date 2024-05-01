package graduateProject.planner.algorithm.planGenerator

trait PhysicalAction

trait BasicAction extends PhysicalAction

trait BeforeAction extends BasicAction
trait AfterAction extends BasicAction

trait CqcAction extends PhysicalAction

trait ReduceAction extends CqcAction
trait EnumerateAction extends CqcAction
