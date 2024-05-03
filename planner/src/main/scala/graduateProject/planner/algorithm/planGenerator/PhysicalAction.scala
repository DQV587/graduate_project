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

trait ArrayByKeyAction extends ReduceAction

case class SourceTableArrayByKeyAction(oldName:String,
                                       newName:String,
                                       key:(Int,DataType)) extends ArrayByKeyAction
case class AggregatedTableArrayByKeyAction(oldName:String,
                                           newName:String,
                                           keyIndex:Int,
                                           valueIndex:Int,
                                           func:String) extends ArrayByKeyAction

case class ReKeyAction(oldName:String,
                       newName:String,
                       key:(Int,DataType)) extends ArrayByKeyAction

case class NoIncidentComparisonsReduce(oldName:String,
                                       newName:String) extends ArrayByKeyAction
trait AppendMfAction extends ArrayByKeyAction
case class AppendKeyValueAction(appendTo:String,appendFrom:String
                                ,newName:String) extends AppendMfAction
case class AppendKey2TupleAction(appendTo:String,appendFrom:String,newName:String,
                                 compareValueIndex:Int,
                                 comparisonIndex:Int,
                                 isLeft:Boolean) extends AppendMfAction

case class SelfFilterAction(oldName:String,
                             newName:String,
                             comparisonIndex:Int,
                             leftIndex:Int,
                             rightIndex:Int) extends ArrayByKeyAction

trait GroupByKeyAction extends ReduceAction
case class KeyArrayGroupByKeyAction(oldName:String,
                                    newName:String) extends GroupByKeyAction

case class SortGroupByKeyAction(oldName:String,
                                newName:String,
                                valueIndex:Int,
                                comparisonIndex:Int,
                                isLeft:Boolean) extends GroupByKeyAction

case class GetMfFromSortedGroupByKeyAction(oldName:String,
                                           newName:String) extends GroupByKeyAction




trait OneDimArrayByKeyAction extends ReduceAction

case class SortByOneDimArrayAction(oldName:String,
                              newName:String,
                              valueIndex1: Int,
                              comparisonIndex1: Int,
                              isLeft1: Boolean,
                              valueIndex2: Int,
                              comparisonIndex2: Int,
                              isLeft2: Boolean
                             ) extends OneDimArrayByKeyAction

case class GetMfFromOneDimArrayAction(oldName:String,
                                      newName:String
                                     ) extends OneDimArrayByKeyAction

trait EnumerateAction extends CqcAction

