package graduateProject.planner.algorithm

import graduateProject.planner.entity.expression.Comparison
import graduateProject.planner.entity.hypergraph.comparisonHypergraph.{ComparisonHyperGraph, ComparisonHyperGraphEdge}
import graduateProject.planner.entity.hypergraph.relationHypergraph.Relation
import graduateProject.planner.entity.joinTree.{JoinTree, JoinTreeEdge}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object JoinTreeToComparisonHyperGraph {
  def apply(joinTree:JoinTree,comparisons:Set[Comparison]):ComparisonHyperGraph={
    val relationSet=joinTree.nodeSet
    val relationJoinTreeEdgeMapBuffer:mutable.Map[Relation,ArrayBuffer[JoinTreeEdge]]=mutable.Map()
    for(relation<-relationSet){
      relationJoinTreeEdgeMapBuffer.put(relation,ArrayBuffer[JoinTreeEdge]())
    }
    for(edge<-joinTree.edgeSet){
      relationJoinTreeEdgeMapBuffer(edge.son).append(edge)
      relationJoinTreeEdgeMapBuffer(edge.father).append(edge)
    }
    val relationJoinTreeEdgeMap=relationJoinTreeEdgeMapBuffer.map(x=>(x._1,x._2.toSet))
    val edgeSet=comparisons.map(comparison=>{
      val leftRelation = relationSet.find(x => comparison.left.getVariables.subsetOf(x.getVariables()))
      val rightRelation = relationSet.find(x => comparison.right.getVariables.subsetOf(x.getVariables()))
      //find a path from left to right, there will be and only be one path which is guaranteed by the property of join tree
      //use dfs
      val path=DFS(leftRelation,rightRelation,relationJoinTreeEdgeMap)
      new ComparisonHyperGraphEdge(comparison = comparison, edges = path, left = leftRelation, right = rightRelation)
    })
    val nodeSet=joinTree.edgeSet
    new ComparisonHyperGraph(joinTree,edgeSet,nodeSet)
  }
  def DFS(from:Option[Relation],to:Option[Relation],edgeMap:mutable.Map[Relation,Set[JoinTreeEdge]]):Set[JoinTreeEdge]= {
    if (from.isEmpty || to.isEmpty) Set[JoinTreeEdge]()
    else {
      val visited = mutable.Set[Relation]()
      val left=from.get
      val right=to.get
      val stk=mutable.Stack[Relation]()
      val pathStk=mutable.Stack[Relation]()
      stk.push(left)
      var getTo=left
      while(stk.nonEmpty&&(!getTo.equals(right))){
        val curNodeToVisit=stk.top
        getTo=curNodeToVisit
        visited.add(curNodeToVisit)
        val curEdgeSetToTraversal=edgeMap(curNodeToVisit)
        var goDeeper=false
        for(edge<-curEdgeSetToTraversal){
          val theOtherNode=(if(edge.father.equals(curNodeToVisit)) edge.son else edge.father)
          if(!visited.contains(theOtherNode)) {
            stk.push(theOtherNode)
            goDeeper = true
          }
        }
        if(goDeeper) pathStk.push(curNodeToVisit)
        else{
          //backtracking phase, if the node's neighbors are all visited and no path is found, this node does not in the path
          val poppedRelation=stk.pop()
          if(pathStk.top.equals(poppedRelation)){
            pathStk.pop()
          }
        }
      }
      var nodesInPath=pathStk.toSet+right
      val edgeBuffer=ArrayBuffer[JoinTreeEdge]()
      var curNode=left
      while(!curNode.equals(right)){
        for(edge<-edgeMap(curNode)){
          val theOtherNode=(if(edge.father.equals(curNode)) edge.son else edge.father)
          if(nodesInPath.contains(theOtherNode)) {
            edgeBuffer.append(edge)
            nodesInPath=nodesInPath-curNode
            curNode=theOtherNode
          }
        }
      }
      edgeBuffer.toSet
    }
  }
}
