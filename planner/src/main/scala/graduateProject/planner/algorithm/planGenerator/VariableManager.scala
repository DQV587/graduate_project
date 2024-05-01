package graduateProject.planner.algorithm.planGenerator

import graduateProject.planner.entity.data_type.DataType
import graduateProject.planner.entity.physicalPlanVariable.{ArrayTypeVariable, VariableInformation}

import scala.collection.mutable

class VariableManager {
  val variableInformationMap=mutable.Map[String,VariableInformation]()
  def get(variableName:String):VariableInformation=variableInformationMap(variableName)
  def register(variableName:String,columns:Array[(String,DataType)]):Unit={
    variableInformationMap(variableName)=ArrayTypeVariable(variableName,columns)
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
    s"v${num}"
  }
}
