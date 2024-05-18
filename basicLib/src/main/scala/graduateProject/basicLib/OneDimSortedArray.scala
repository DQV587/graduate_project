package graduateProject.basicLib

/**
 * @param input the input Array to build the TreeLikeArray
 * @param getValueFun1
 * @param getValueFun2
 * @param sortFunc1
 * @param sortFunc2
 * @tparam C1 type of arguments in the first compare function
 * @tparam C2 type of arguments in the second compare function
 */
class OneDimSortedArray[C1, C2](input: Array[Array[Any]],
                                getValueFun1: (Array[Any])=>C1,
                                getValueFun2: (Array[Any])=>C2,
                                sortFunc1: (C1, C1) => Boolean,
                                sortFunc2: (C2, C2) => Boolean) extends java.io.Serializable {
    // sort by the first compare function on position keyIndex1. The value located at keyIndex1 is T1(may not = C1).
    // Use the implicit f1 to convert it into type C1 and compare
    val content = input.sortWith((x, y) => sortFunc1(getValueFun1(x),getValueFun1(y)))

    class OneDimSortedArray(value1: C1, value2: C2) extends Iterator[Array[Any]] {
        var keyValue1: C1 = value1
        var keyValue2: C2 = value2
        private var currentRowIndex: Int = 0
        private var nextElement: Array[Any] = _

        override def hasNext: Boolean = {
            if (nextElement != null)
                true
            else {
                while (currentRowIndex < content.length && nextElement == null) {
                    val row = content(currentRowIndex)
                    currentRowIndex = currentRowIndex + 1
                    if (sortFunc1(getValueFun1(row), keyValue1)) {
                        if (sortFunc2(getValueFun2(row), keyValue2)) {
                            nextElement = row
                        }
                    } else {
                        currentRowIndex = content.length
                    }
                }

                nextElement != null
            }
        }

        override def next(): Array[Any] = {
            if (!hasNext)
                Iterator.empty.next()
            else {
                val result = nextElement
                nextElement = null
                result
            }
        }
    }

    def iterator(keyValue1: C1, keyValue2: C2): Iterator[Array[Any]] = new OneDimSortedArray(keyValue1, keyValue2)

    def getMin: Array[(C1, C2)] = {
        val size = content.length
        val result = Array.ofDim[(C1, C2)](size)
        val curEle=content(0)
        result(0) = (getValueFun1(curEle), getValueFun2(curEle))
        for (i <- 1 until size) {
          val curEle=content(i)
            if (sortFunc2(result(i - 1)._2, getValueFun2(curEle))) {
                result(i) = (getValueFun1(curEle), result(i - 1)._2)
            } else {
                result(i) = (getValueFun1(curEle), getValueFun2(curEle))
            }
        }
        result
    }
}
