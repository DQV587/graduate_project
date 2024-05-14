package graduateProject.planner.algorithm.planGenerator

import graduateProject.parser.implLib.ddl.SqlTable.TableColumn
import graduateProject.planner.entity.dataType.DataType
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


trait CqcAction extends PhysicalAction

trait ReduceAction extends CqcAction
trait ArrayAction extends ReduceAction
case class SourceTableArrayByKeyAction(oldName:String,
                                       newName:String,
                                       key:(Int,DataType)) extends ArrayAction
case class AggregatedTableArrayByKeyAction(oldName:String,
                                           newName:String,
                                           keyIndex:Int,
                                           valueIndex:Int,
                                           dataType: DataType) extends ArrayAction

trait ArrayByKeyAction extends ReduceAction


case class ReKeyAction(oldName:String,
                       newName:String,
                       key:(Int,DataType)) extends ArrayByKeyAction

case class NoIncidentComparisonsReduce(oldName:String,
                                       newName:String,
                                       otherName:String) extends ArrayByKeyAction
trait AppendMfAction extends ArrayByKeyAction
case class AppendKeyValueAction(appendTo:String,appendFrom:String
                                ,newName:String) extends AppendMfAction
case class AppendKey2TupleAction(appendTo:String,appendFrom:String,newName:String,
                                 compareValueIndex:Int,
                                 comparisonIndex:Int,
                                 dataType1: DataType,
                                 dataType2: DataType,
                                 isLeft:Boolean) extends AppendMfAction

case class SelfFilterAction(oldName:String,
                             newName:String,
                             comparisonIndex:Int,
                             leftIndex:Int,
                             rightIndex:Int,
                             dataType: DataType) extends ArrayByKeyAction

trait GroupByKeyAction extends ReduceAction
case class KeyArrayGroupByKeyAction(oldName:String,
                                    newName:String) extends GroupByKeyAction

case class SortGroupByKeyAction(oldName:String,
                                newName:String,
                                value:(Int,DataType),
                                comparisonIndex:Int,
                                isLeft:Boolean) extends GroupByKeyAction

case class GetMfFromSortedGroupByKeyAction(oldName:String,
                                           newName:String,
                                           value:(Int,DataType)) extends GroupByKeyAction




trait OneDimArrayByKeyAction extends ReduceAction

case class SortByOneDimArrayAction(oldName:String,
                              newName:String,
                              value1: (Int,DataType),
                              comparisonIndex1: Int,
                              isLeft1: Boolean,
                              value2: (Int,DataType),
                              comparisonIndex2: Int,
                              isLeft2: Boolean
                             ) extends OneDimArrayByKeyAction

case class GetMfFromOneDimArrayAction(oldName:String,
                                      newName:String
                                     ) extends OneDimArrayByKeyAction

trait EnumerateAction extends CqcAction
case class EnumerateWithNoComparisonAction(oldName:String, newName:String, otherName:String,
                                            thisIndices:List[Int], otherIndices:List[Int],
                                            keyType:DataType) extends EnumerateAction

case class EnumerateWithOneComparisonAction(oldName: String, newName: String, otherName: String,
                                            valueIndex1:Int,valueIndex2:Int,
                                            comparisonIndex:Int,isLeft:Boolean,comparisonType:DataType,
                                            thisIndices: List[Int], otherIndices: List[Int],
                                            keyType: DataType) extends EnumerateAction
case class EnumerateWithTwoComparisonsAction(oldName:String, newName:String, otherName:String,
                                             valueIndex1:Int, valueType1:DataType,
                                             valueIndex2:Int, valueType2:DataType,
                                             thisIndices: List[Int], otherIndices: List[Int],
                                             keyType:DataType
                                           ) extends EnumerateAction

case class CompleteAction(oldName:String, newName:String) extends CqcAction
trait AfterAction extends BasicAction
case class CountResultAction(resultName:String) extends AfterAction
case class PersistResultAction(newName:String,resultName:String,outputMap: List[Int]) extends AfterAction
