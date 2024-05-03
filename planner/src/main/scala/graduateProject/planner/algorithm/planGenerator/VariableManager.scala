package graduateProject.planner.algorithm.planGenerator

import graduateProject.planner.entity.data_type.DataType
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
