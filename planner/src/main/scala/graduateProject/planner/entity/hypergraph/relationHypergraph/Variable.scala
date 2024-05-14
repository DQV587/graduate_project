package graduateProject.planner.entity.hypergraph.relationHypergraph

import graduateProject.planner.entity.dataType.DataType

class Variable(val name: String,val dataType: DataType) {
  def getName:String=this.name
  def getType:DataType=this.dataType

  override def toString: String = this.name
}

object Variable{
  private var count: Int = 0;
  def newVariable(dataType: DataType):Variable={
    count+=1;
    new Variable(s"v${count}",dataType)
  }
  def reset(): Unit =this.count=0
}
