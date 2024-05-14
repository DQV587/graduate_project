package graduateProject.planner.entity.physicalPlanVariable

import graduateProject.planner.entity.dataType.DataType

trait VariableInformation{
  val name:String
  def getVariableName:String=name
}

case class ArrayTypeVariable(name:String,columns:Array[(String,DataType)]) extends VariableInformation{
  override def toString: String = {
    name+" : "+columns.mkString("(",",",")")
  }
}

case class KeyArrayTypeVariable(name:String,keyIndex:Int,columns:Array[(String,DataType)]) extends VariableInformation{
  override def toString: String = {
    name + " : " +"keyIndex: "+keyIndex+" value: "+ columns.mkString("(", ",", ")")
  }
}

case class KeyGroupByTypeVariable(name:String,keyIndex:Int,columns:Array[(String,DataType)],sorted:Boolean,sortedByIndex:Int) extends VariableInformation{
  override def toString: String = {
    val sortedInformation=if(sorted) "sorted by index "+sortedByIndex else "not sorted"
    name + " : " + "grouped by keyIndex: " + keyIndex + " value: " + columns.mkString("(", ",", ")")+sortedInformation
  }
}

case class KeyOneDimArrayTypeVariable(name:String,keyIndex:Int,columns:Array[(String,DataType)],
                                      valueIndex1:Int,comparisonIndex1:Int,isLeft1:Boolean,
                                      valueIndex2:Int,comparisonIndex2:Int,isLeft2:Boolean) extends VariableInformation


case class KeyValueTypeVariable(name:String,key:(String,DataType),value:(String,DataType)) extends VariableInformation{
  override def toString: String = {
    name+" : "+"(key: "+key+ " value: "+value+")"
  }
}

case class KeyTwoValueTypeVariable(name:String,key:(String,DataType),
                                   value1:(String,DataType),value2:(String,DataType)) extends VariableInformation
