package graduateProject.planner

import graduateProject.parser.CatalogManager
import graduateProject.parser.implLib.SQLParser
import graduateProject.parser.plan.SqlPlanner
import graduateProject.planner.codeGenerator.GenerateCode
import graduateProject.planner.entity.hypergraph.relationHypergraph.RelationHyperGraph
import graduateProject.planner.innerRepresentation.{GYO, JoinTreeToComparisonHyperGraph, RelNodeToQuery}
import graduateProject.planner.optimizer.RedundantPlanRemove
import graduateProject.planner.planGenerator.{PhysicalPlanGenerator, ReducePlanGenerator}

import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.io.Source
object Main {
  def main(args: Array[String]) {
    val sqlPath="sql/sql3"
    val ddlPath=sqlPath+"/ddl.txt"
    val dmlPath=sqlPath+"/dml.txt"
    val ddlSource=Source.fromFile(ddlPath)
    val ddl = ddlSource.mkString
    ddlSource.close()
    val dmlSource = Source.fromFile(dmlPath)
    val dml= dmlSource.mkString
    dmlSource.close()
    val tmp = SQLParser.parseDml(dml)
    val nodeList = SQLParser.parseDdl(ddl)
    val catalogManager = new CatalogManager
    catalogManager.register(nodeList)
    val crownPlanner = new SqlPlanner(catalogManager)
    val root = crownPlanner.toLogicalPlan(tmp)
    val query=RelNodeToQuery.convert(root)
    val hyperGraph=RelationHyperGraph.constructFromQuery(query)
    println(hyperGraph.isAcyclic)
    val joinTreeSet=GYO(hyperGraph)
    val comparisonHyperGraphSet=joinTreeSet.map(joinTree=>JoinTreeToComparisonHyperGraph(joinTree,query.comparisons.toSet))

    val acyclicCHG=comparisonHyperGraphSet.filter(chg=>chg.isBergeAcyclic)
    println(acyclicCHG.size)
    val comparisonHyperGraph=acyclicCHG.head
    val reducePlanSet=ReducePlanGenerator(comparisonHyperGraph)
    println(reducePlanSet.size)
    val newPlanList=RedundantPlanRemove(reducePlanSet)
    println(newPlanList.size)
    val reducePlan = newPlanList.last
    val physicalPlan = PhysicalPlanGenerator(catalogManager, query, reducePlan)
    val builder = new mutable.StringBuilder()
    GenerateCode(physicalPlan, builder)
    val write = new PrintWriter(new File(s"experiment/src/main/scala/QueryProcess.scala"))
    write.write(builder.toString())
    write.close()
//    for(i <- newPlanList.indices){
//      val reducePlan=newPlanList(i)
//      val physicalPlan = PhysicalPlanGenerator(catalogManager, query, reducePlan)
//      val builder = new mutable.StringBuilder()
//      GenerateCode(physicalPlan, builder)
//      val write = new PrintWriter(new File(s"experiment/src/main/scala/QueryProcessPlan${i}.scala"))
//      write.write(builder.toString())
//      write.close()
//    }

  }
}