package graduateProject.planner.algorithm.planGenerator

import graduateProject.planner.entity.dataType.DataType
import graduateProject.planner.entity.physicalPlanVariable._

import scala.collection.mutable

class VariableManager {
  private val variableInformationMap=mutable.Map[String,VariableInformation]()
  def get(variableName:String):VariableInformation=variableInformationMap(variableName)
  def register(variableName:String,columns:Array[(String,DataType)]):Unit={
    variableInformationMap(variableName)=ArrayTypeVariable(variableName,columns)
  }
  def register(variableName:String,key:(String,DataType),value:(String,DataType)):Unit={
    variableInformationMap(variableName)=KeyValueTypeVariable(variableName,key,value)
  }
  def register(variableName:String,key:Int,columns:Array[(String,DataType)]):Unit={
    variableInformationMap(variableName)=KeyArrayTypeVariable(variableName,key,columns)
  }

  def register(variableName: String, key: Int, columns: Array[(String, DataType)],sorted:Boolean): Unit = {
    variableInformationMap(variableName) = KeyGroupByTypeVariable(variableName, key, columns,sorted,0)
  }

  def register(variableName: String, key: Int, columns: Array[(String, DataType)], sorted: Boolean,sortIndex:Int): Unit = {
    variableInformationMap(variableName) = KeyGroupByTypeVariable(variableName, key, columns, sorted, sortIndex)
  }
  def register(variableName: String, key: Int, columns: Array[(String, DataType)],
               valueIndex1: Int, comparisonIndex1: Int, isLeft1: Boolean,
               valueIndex2: Int, comparisonIndex2: Int, isLeft2: Boolean):Unit={
    variableInformationMap(variableName)=KeyOneDimArrayTypeVariable(variableName,key,columns,
      valueIndex1, comparisonIndex1, isLeft1,
      valueIndex2, comparisonIndex2, isLeft2)
  }
  def register(variableName: String, key: (String, DataType),
               index: (String, DataType), value: (String, DataType)):Unit={
    variableInformationMap(variableName)=KeyTwoValueTypeVariable(variableName,key,index,value)
  }
  override def toString: String = {
    variableInformationMap.mkString("VariableInformation:\r\n","\r\n"," ")
  }
//  def register()
}
object VariableManager{
  private var num=0;
  def getNewVariableName:String={
    num+=1;
    s"rdd${num}"
  }
}
