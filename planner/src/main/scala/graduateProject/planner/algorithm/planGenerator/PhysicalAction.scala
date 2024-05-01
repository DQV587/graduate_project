package graduateProject.planner.algorithm.planGenerator

import graduateProject.parser.implLib.ddl.SqlTable.TableColumn
import graduateProject.planner.entity.data_type.DataType
import graduateProject.planner.entity.expression.{ComparisonOperator, Expression}

trait PhysicalAction

trait BasicAction extends PhysicalAction
trait BeforeAction extends BasicAction
case class CreateTableFromFileAction(variableName:String,
                                     columns:Array[TableColumn],
                                     filePath:String,
                                     delimiter:String) extends BeforeAction

case class CalculateAggregateTableAction(variableName:String,
                                         sourceVariable:String,
                                         groupByIndex:Int,
                                         aggregateFunc:String
                                        ) extends BeforeAction

case class CreateComparisonFunctionAction(index:Int,
                                          operator:ComparisonOperator,
                                          leftExpr:Expression,
                                          rightExpr:Expression,
                                          dataType: DataType) extends BeforeAction

case  class CreateSortComparisonFunctionAction(index:Int,
                                               operator: ComparisonOperator,
                                               dataType: DataType) extends BeforeAction

trait AfterAction extends BasicAction

trait CqcAction extends PhysicalAction

trait ReduceAction extends CqcAction

trait GroupByKeyAction extends ReduceAction

case class SourceTableGroupByKeyAction(oldName:String,
                                       newName:String,
                                       key:(Int,DataType)) extends GroupByKeyAction
case class AggregatedTableGroupByKeyAction(oldName:String,
                                           newName:String,
                                           keyIndex:Int,
                                           valueIndex:Int,
                                           func:String) extends GroupByKeyAction

case class ReKeyAction(oldName:String,
                       newName:String,
                       key:(Int,DataType)) extends GroupByKeyAction

case class AppendMfAction(appendTo:String,appendFrom:String
                         ,newName:String) extends ReduceAction
trait EnumerateAction extends CqcAction
