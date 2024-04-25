package graduateProject.planner.entity.hypergraph

trait HyperGraphEdge[V] {
  var nodeSet: Set[V]

  def addNode(node: V): Unit = this.nodeSet=nodeSet + node
  def addNodes(nodes:Set[V]):Unit=this.nodeSet=this.nodeSet++nodes
  def removeNode(node:V):Unit=this.nodeSet=this.nodeSet-node
  def removeNodes(nodes:Set[V]):Unit=this.nodeSet=this.nodeSet--nodes

  def getNodes: Set[V] = this.nodeSet

  def isEmpty: Boolean = this.nodeSet.isEmpty

  def nonEmpty: Boolean = this.nodeSet.nonEmpty

  def getNodesNum: Int = this.nodeSet.size
}
