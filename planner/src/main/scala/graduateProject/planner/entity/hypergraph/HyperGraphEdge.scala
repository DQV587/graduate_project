package graduateProject.planner.entity.hypergraph

trait HyperGraphEdge[V] {
  val nodeSet: Set[V]

  def addNode(node: V): Unit = this.nodeSet + node

  def getNodes: Set[V] = this.nodeSet

  def isEmpty: Boolean = this.nodeSet.isEmpty

  def nonEmpty: Boolean = this.nodeSet.nonEmpty

  def getNodesNum: Int = this.nodeSet.size
}
