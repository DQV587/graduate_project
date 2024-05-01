package graduateProject.planner.algorithm.planGenerator

import graduateProject.parser.CatalogManager
import graduateProject.parser.implLib.ddl.SqlTable
import graduateProject.planner.entity.data_type.{DataType, LongDataType}
import graduateProject.planner.entity.expression.Comparison
import graduateProject.planner.entity.hypergraph.comparisonHypergraph.{ComparisonHyperGraph, ReduceInformation}
import graduateProject.planner.entity.hypergraph.relationHypergraph.{AggregatedRelation, Relation, TableScanRelation}
import graduateProject.planner.entity.physicalPlanVariable.ArrayTypeVariable
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
  def apply(catalog: CatalogManager, query: Query, comparisonHyperGraph: ComparisonHyperGraph): Unit = {
    val relationMapToVariable=mutable.Map[Relation,String]()
    val comparisonMapToInt=mutable.Map[Comparison,Int]()
    val variableManager=new VariableManager()

    //Before actions

    // create table from file
    val relations = query.relations
    val sourceTableNames = getSourceTableNames(relations.toSet)
    val tables = sourceTableNames.map(tableName => catalog.getSchema.getTable(tableName, false).getTable).map(t => t.asInstanceOf[SqlTable])
    val beforeActionList = mutable.ListBuffer[BeforeAction]()
    //read file and create table view  AND register the table variable name
    tables.foreach(table=>{
      beforeActionList.append(
      CreateTableFromFileAction(table.getTableName,
        table.getTableColumns,
        table.getTableProperties.get("path"),
        table.getTableProperties.get("delimiter")))
      val columnsInformation=mutable.ArrayBuffer[(String,DataType)]()
      table.getTableColumns.foreach(x=>
        columnsInformation.append((x.getName,DataType.fromTypeName(x.getType))))
      variableManager.register(table.getTableName,columnsInformation.toArray)
    })
    relations.filter(relation=>relation.isInstanceOf[TableScanRelation])
      .foreach(relation=>relationMapToVariable(relation)=relation.getTableName())

    // create table from exist table by aggregate function
    val aggregatedRelations=getAggregateRelations(relations.toSet)
    val aggregatedTableSet=aggregatedRelations.map(relation=>getTuple3FromAggregatedRelation(relation))
    val aggregatedTableToVariableMap=aggregatedTableSet.map(tuple3=>
      (tuple3,tuple3._1+"AggregatedWithValue"+tuple3._2.toString+"By"+tuple3._3)).toMap
    aggregatedTableSet.foreach(table=>{
      // generate action
      beforeActionList.append(
        CalculateAggregateTableAction(aggregatedTableToVariableMap(table),
          table._1, table._2,table._3)
      )
      // register variable and bind the relation to that
      val columnInformation=ArrayBuffer[(String,DataType)]()
      columnInformation.append(
        (variableManager.get(table._1).asInstanceOf[ArrayTypeVariable].columns(table._2)._1,
        variableManager.get(table._1).asInstanceOf[ArrayTypeVariable].columns(table._2)._2))
      columnInformation.append(("aggregateFuncValue",LongDataType))
      variableManager.register(aggregatedTableToVariableMap(table),columnInformation.toArray)
    })
    aggregatedRelations.foreach(relation=>{
      val tuple3=getTuple3FromAggregatedRelation(relation)
      relationMapToVariable(relation)=aggregatedTableToVariableMap(tuple3)
    })

    // parse comparison
    val comparisons=query.comparisons
    var comparisonIndex=0
    comparisons.foreach(comparison=>{
      comparisonIndex+=1
      comparisonMapToInt(comparison)=comparisonIndex
      beforeActionList.append(
        CreateComparisonFunctionAction(comparisonMapToInt(comparison),
          comparison.operator,
          comparison.left,comparison.right,
          comparison.data_type)
      )
      beforeActionList.append(
        CreateSortComparisonFunctionAction(comparisonMapToInt(comparison),
          comparison.operator,
          comparison.data_type)
      )
    })


    val reduceInformationArray = mutable.ArrayBuffer[ReduceInformation]()
    val nodeNum = comparisonHyperGraph.nodeSet.size
    for (i <- 0 to nodeNum) {
      val reducibleRelationSet = comparisonHyperGraph.getReducibleRelations
      val information = comparisonHyperGraph.reduceRelation(reducibleRelationSet.head)
      reduceInformationArray.append(information)
    }
//    reduceInformationArray.foreach(x => println(x))
    //    PhysicalPlan()
  }
}

case class ReduceState(informationList:List[ReduceInformation],chg:ComparisonHyperGraph)
