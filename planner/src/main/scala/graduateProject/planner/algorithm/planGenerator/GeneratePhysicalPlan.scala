package graduateProject.planner.algorithm.planGenerator

import graduateProject.parser.CatalogManager
import graduateProject.parser.implLib.ddl.SqlTable
import graduateProject.planner.entity.data_type.{DataType, LongDataType}
import graduateProject.planner.entity.expression.Comparison
import graduateProject.planner.entity.hypergraph.comparisonHypergraph.{ComparisonHyperGraph, ReduceInformation}
import graduateProject.planner.entity.hypergraph.relationHypergraph.{AggregatedRelation, Relation, TableScanRelation}
import graduateProject.planner.entity.physicalPlanVariable.{ArrayTypeVariable, KeyArrayTypeVariable}
import graduateProject.planner.entity.query.Query

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object GeneratePhysicalPlan {

  def getSourceTableNames(relations: Set[Relation]):Set[String]={
    relations.map(relation=>relation.getTableName())
  }
  def getAggregateRelations(relations: Set[Relation]):Set[AggregatedRelation]={
    relations.filter(relation=>relation.isInstanceOf[AggregatedRelation])
      .map(relation=>relation.asInstanceOf[AggregatedRelation])
  }
  def getTuple3FromAggregatedRelation(relation: AggregatedRelation):(String,Int,String)={
    (relation.getTableName(),relation.group.head,relation.func)
  }
  def getBeforeActions(catalog: CatalogManager, query: Query,
    relationMapToVariable:mutable.Map[Relation, String],
    comparisonMapToInt:mutable.Map[Comparison, Int],
    variableManager:VariableManager):List[BeforeAction]={
    val relations = query.relations
    val sourceTableNames = getSourceTableNames(relations.toSet)
    val tables = sourceTableNames.map(tableName => catalog.getSchema.getTable(tableName, false).getTable).map(t => t.asInstanceOf[SqlTable])
    val beforeActionList = mutable.ListBuffer[BeforeAction]()
    //read file and create table view  AND register the table variable name
    tables.foreach(table => {
      beforeActionList.append(
        CreateTableFromFileAction(table.getTableName,
          table.getTableColumns,
          table.getTableProperties.get("path"),
          table.getTableProperties.get("delimiter")))
      val columnsInformation = mutable.ArrayBuffer[(String, DataType)]()
      table.getTableColumns.foreach(x =>
        columnsInformation.append((x.getName, DataType.fromTypeName(x.getType))))
      variableManager.register(table.getTableName, columnsInformation.toArray)
    })
    relations.filter(relation => relation.isInstanceOf[TableScanRelation])
      .foreach(relation => relationMapToVariable(relation) = relation.getTableName())

    // create table from exist table by aggregate function
    val aggregatedRelations = getAggregateRelations(relations.toSet)
    val aggregatedTableSet = aggregatedRelations.map(relation => getTuple3FromAggregatedRelation(relation))
    val aggregatedTableToVariableMap = aggregatedTableSet.map(tuple3 =>
      (tuple3, tuple3._1 + "AggregatedWithValue" + tuple3._2.toString + "By" + tuple3._3)).toMap
    aggregatedTableSet.foreach(table => {
      // generate action
      beforeActionList.append(
        CalculateAggregateTableAction(aggregatedTableToVariableMap(table),
          table._1, table._2, table._3)
      )
      // register variable and bind the relation to that
      val columnInformation = ArrayBuffer[(String, DataType)]()
      columnInformation.append(
        (variableManager.get(table._1).asInstanceOf[ArrayTypeVariable].columns(table._2)._1,
          variableManager.get(table._1).asInstanceOf[ArrayTypeVariable].columns(table._2)._2))
      columnInformation.append(("aggregateFuncValue", LongDataType))
      variableManager.register(aggregatedTableToVariableMap(table), columnInformation.toArray)
    })
    aggregatedRelations.foreach(relation => {
      val tuple3 = getTuple3FromAggregatedRelation(relation)
      relationMapToVariable(relation) = aggregatedTableToVariableMap(tuple3)
    })

    // parse comparison
    val comparisons = query.comparisons
    var comparisonIndex = 0
    comparisons.foreach(comparison => {
      comparisonIndex += 1
      comparisonMapToInt(comparison) = comparisonIndex
      beforeActionList.append(
        CreateComparisonFunctionAction(comparisonMapToInt(comparison),
          comparison.operator,
          comparison.left, comparison.right,
          comparison.data_type)
      )
      beforeActionList.append(
        CreateSortComparisonFunctionAction(comparisonMapToInt(comparison),
          comparison.operator,
          comparison.data_type)
      )
    })
    beforeActionList.toList
  }
  def getCqcActions(query: Query, reduceInformationList:List[ReduceInformation],
                    relationMapToVariable: mutable.Map[Relation, String],
                    comparisonMapToInt: mutable.Map[Comparison, Int],
                    variableManager: VariableManager ):List[CqcAction]={
    val cqcActions=mutable.ListBuffer[CqcAction]()
    reduceInformationList.foreach(reduceInformation=>{
      reduceInformation.relation match {
        case aggregatedRelation: AggregatedRelation=>{
          val thisRelation=aggregatedRelation
          val thisRelationVariable=variableManager.get(relationMapToVariable(thisRelation))
            .asInstanceOf[ArrayTypeVariable]
          val newName=VariableManager.getNewVariableName
          cqcActions.append(AggregatedTableGroupByKeyAction(thisRelationVariable.name,
            newName,0,1,thisRelation.func))
          relationMapToVariable(thisRelation)=newName
          val joinTreeEdge=reduceInformation.reducedJoinTreeEdge.get
          val sharedVariable=joinTreeEdge.sharedVariable.head
          val valueVariable=(thisRelation.nodeSet-sharedVariable).head
          variableManager.register(newName,
            (sharedVariable.name,sharedVariable.dataType),(valueVariable.name,valueVariable.dataType))

          val otherRelation=joinTreeEdge.getOtherRelation(thisRelation)
          val otherRelationVariableName=relationMapToVariable(otherRelation)
          val otherRelationVariable=variableManager.get(otherRelationVariableName)
          otherRelationVariable match {
            case arrayType:ArrayTypeVariable=>{
              val columnArray=mutable.ArrayBuffer[(String,DataType)]()
              var index=0
              var i=0
              for(variable<-otherRelation.getVariables()){
                columnArray.append((variable.name,variable.dataType))
                if(!variable.name.equals(sharedVariable.name)){
                  i+=1
                }
                else{
                  index=i
                }
              }
              val newName=VariableManager.getNewVariableName
              val columns=columnArray.toArray
              cqcActions.append(
                SourceTableGroupByKeyAction(otherRelationVariable.name,newName,
                  (index,columns(index)._2))
              )
              relationMapToVariable(otherRelation)=newName
              variableManager.register(newName,index,columns)
            }
            case keyArrayType:KeyArrayTypeVariable=>{
              // has different key for the two variable, so the other relation need to perform reKey action
              if(!keyArrayType.columns(keyArrayType.keyIndex)._1.equals(sharedVariable.name)){
                val newName=VariableManager.getNewVariableName
                var i=0
                var index=0
                for(variable<-otherRelation.getVariables()){
                  if (!variable.name.equals(sharedVariable.name)) {
                    i += 1
                  }
                  else {
                    index = i
                  }
                }
                cqcActions.append(ReKeyAction(otherRelationVariableName,newName,(index,keyArrayType.columns(index)._2)))
                relationMapToVariable(otherRelation)=newName
                variableManager.register(newName,index,keyArrayType.columns)
              }
            }
          }
          val afterAppendName=VariableManager.getNewVariableName
          cqcActions.append(AppendMfAction(relationMapToVariable(otherRelation),
            relationMapToVariable(thisRelation),afterAppendName))
          val otherRelationVariable1=variableManager.get(relationMapToVariable(otherRelation)).asInstanceOf[KeyArrayTypeVariable]
          variableManager.register(afterAppendName,otherRelationVariable1.keyIndex,
            otherRelationVariable1.columns:+(valueVariable.name,valueVariable.dataType))
          relationMapToVariable(otherRelation)=afterAppendName
      }
        case tableScanRelation: TableScanRelation => {

        }
        case _=>throw new UnsupportedOperationException("not support this type Relation")
      }
    })
    cqcActions.toList
  }
  def apply(catalog: CatalogManager, query: Query, comparisonHyperGraph: ComparisonHyperGraph): Unit = {
    val relationMapToVariable=mutable.Map[Relation,String]()
    val comparisonMapToInt=mutable.Map[Comparison,Int]()
    val variableManager=new VariableManager()

    val beforeAction=getBeforeActions(catalog, query,
      relationMapToVariable, comparisonMapToInt, variableManager)

    // create table from file

    val reduceInformationArray = mutable.ArrayBuffer[ReduceInformation]()
    val nodeNum = comparisonHyperGraph.nodeSet.size
    for (i <- 0 to nodeNum) {
      val reducibleRelationSet = comparisonHyperGraph.getReducibleRelations
      val information = comparisonHyperGraph.reduceRelation(reducibleRelationSet.head)
      reduceInformationArray.append(information)
    }
    val cqcAction=getCqcActions(query,reduceInformationArray.take(3).toList,relationMapToVariable,
      comparisonMapToInt, variableManager)
    println(cqcAction)
    println(variableManager)
    println(relationMapToVariable)
//    reduceInformationArray.foreach(x => println(x))
    //    PhysicalPlan()
  }
}
