package graduateProject.basicLib

import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CqcConversions {

  /**
   * K:the origin type of rdd's key
   * T:the new type of rdd's key
   * C:the type of compare element
   * @param rdd
   * @param classTag$K$0
   * @tparam K
   */
  implicit class KeyTupleRDD[K:ClassTag](rdd:RDD[(K,Array[Any])]) extends Serializable {

    def groupBy():RDD[(K,Array[Array[Any]])]={
      rdd.groupByKey().mapValues(x=>x.toArray)
    }

    def reKeyBy[T](keyIndict:Array[Any]=>T):RDD[(T,Array[Any])]={
      rdd.map(x=>(keyIndict(x._2),x._2))
    }

    def semiJoin(other:RDD[(K,Array[Array[Any]])]):RDD[(K,Array[Any])]={
      rdd.cogroup(other).filter(x=>x._2._2.nonEmpty).flatMapValues(x=>x._1)
    }

    def enumerateWithNoComparison[T](other:RDD[(K,Array[Array[Any]])], indices1:Array[Int], indices2:Array[Int],
                                     newKeySelector:(Array[Any],Array[Any])=>T = null):RDD[(T,Array[Any])]={
      rdd.cogroup(other).filter(x=>x._2._1.nonEmpty).mapPartitions(
        iter=>iter.flatMap(
          keyGroup=>{
            val oldKey=keyGroup._1
            val leftIter=keyGroup._2._1
            val rightIter=keyGroup._2._2.head
            leftIter.flatMap(l=>{
              rightIter.map(r=>{
                val newTuple=extractFields(l,r,indices1, indices2)
                if(newKeySelector==null)
                  (oldKey.asInstanceOf[T],newTuple)
                else
                  (newKeySelector(l,r),newTuple)
              })
            })
          }
        )
      )
    }

    def appendMf(mf:RDD[(K,Any)]):RDD[(K,Array[Any])]={
      rdd.cogroup(mf).flatMap(y=>{
        val x=y._2
        val key=y._1
        val lIter=x._1.iterator
        val rIter=x._2
        val newTuple=for{
          l<-lIter
          r<-rIter
        } yield (key,l:+r)
        newTuple
      })
    }

    def enumerateWithOneComparison[T,C](other:RDD[(K,Array[Array[Any]])],
                                        getValueFunc1:(Array[Any])=>C, getValueFunc2:(Array[Any])=>C,
                                        compareFunc:(C,C)=>Boolean,
                                        indices1:Array[Int],indices2:Array[Int],
                                        newKeySelector:(Array[Any],Array[Any])=>T = null):RDD[(T,Array[Any])]={
      rdd.cogroup(other).filter(x=>x._2._1.nonEmpty).mapPartitions(x=>x.flatMap(iter=>{
        val key=iter._1
        val lIter=iter._2._1.toIterator
        val rIter=iter._2._2.head
        lIter.flatMap(l=>{
          rIter.takeWhile(r=>compareFunc(getValueFunc1(l),getValueFunc2(r)))
            .map(r=>{
            val newTuple=extractFields(l,r,indices1,indices2)
            val newKey={
              if(newKeySelector==null) key
              else newKeySelector(l,r)
            }
            (newKey.asInstanceOf[T],newTuple)
          })
        })
      }))
    }
    def appendMf[C1,C2](mf:RDD[(K,Array[(C1,C2)])],
                        getValueFunc:(Array[Any])=>C1,
                        compareFunc:(C1,C1)=>Boolean):RDD[(K,Array[Any])]={
      rdd.cogroup(mf).flatMap(y=>{
        val key=y._1
        val lIter=y._2._1.toIterator
        val rArrayIter=y._2._2
        //隐形一次半连接
        val newTuple=for{
          l<-lIter
          r<-rArrayIter
          //实际上是一次短比较
          if(compareFunc(r.head._1,getValueFunc(l)))
        } yield (key,l:+binarySearchInDictionary(r,getValueFunc(l),compareFunc))
        newTuple
      })
    }
    def enumerateWithTwoComparisons[T,C1,C2](other:RDD[(K,OneDimSortedArray[C1,C2])],
                                             getValueFun1: (Array[Any]) => C1, getValueFun2: (Array[Any]) => C2,
                                             indices1: Array[Int], indices2: Array[Int],
                                             newKeySelector: (Array[Any], Array[Any]) => T = null): RDD[(T, Array[Any])] ={
      rdd.cogroup(other).filter(x=>x._2._1.nonEmpty).mapPartitions(x=>x.flatMap(iter=>{
        val key=iter._1
        val lIter=iter._2._1.toIterator
        val rArray=iter._2._2.head
        lIter.flatMap(l=>{
          val result=rArray.iterator(getValueFun1(l),getValueFun2(l)).map(
            r=>{
            val newTuple=extractFields(l,r,indices1,indices2)
            val newKey=if(newKeySelector==null) key
            else newKeySelector(l,r)
            (newKey.asInstanceOf[T],newTuple)
          })
          result
        })
      }))
    }
    // use one-D structure and filter in enumerate phase
    def enumerateWithMoreComparisons[T, C1, C2](other: RDD[(K, OneDimSortedArray[C1, C2])],
                                               getValueFun1: (Array[Any]) => C1, getValueFun2: (Array[Any]) => C2,
                                               compareFun: (Array[Any], Array[Any]) => Boolean,
                                               indices1: Array[Int], indices2: Array[Int],
                                               newKeySelector: (Array[Any], Array[Any]) => T = null): RDD[(T, Array[Any])] = {
      rdd.cogroup(other).filter(x => x._2._1.nonEmpty).mapPartitions(x => x.flatMap(iter => {
        val key = iter._1
        val lIter = iter._2._1.toIterator
        val rArray = iter._2._2.head
        lIter.flatMap(l => {
          val result = rArray.iterator(getValueFun1(l), getValueFun2(l)).filter(r=>compareFun(l,r))
            .map(r => {
            val newTuple = extractFields(l, r, indices1, indices2)
            val newKey = if (newKeySelector == null) key
            else newKeySelector(l, r)
            (newKey.asInstanceOf[T], newTuple)
          })
          result
        })
      }))
    }
    def appendMf(): Unit ={
      //TODO
    }
    // use multi dim structure
    def enumerateWithMoreComparisons[T, C1, C2](): Unit = {
    //TODO
    }
  }

  implicit class KeyGroupRDD[K:ClassTag](rdd:RDD[(K,Array[Array[Any]])]) extends  Serializable {

    def sortWith[C](getValueFunc:(Array[Any])=>C,compareFunc:(C,C)=>Boolean):RDD[(K,Array[Array[Any]])]={
      rdd.mapValues(value=>value.sortWith((l,r)=>
        compareFunc(getValueFunc(l),getValueFunc(r))))
    }

    def getMf(getValueFunc:(Array[Any])=>Any):RDD[(K,Any)]={
      rdd.mapValues(x=>getValueFunc(x.head))
    }

    def sortByOneDimArray[C1,C2](getValueFun1: (Array[Any]) => C1, getValueFun2: (Array[Any]) => C2,
                                 compareFun1: (C1, C1) => Boolean, compareFun2: (C2, C2) => Boolean)
    :RDD[(K,OneDimSortedArray[C1,C2])]={
      rdd.mapValues(x=>new OneDimSortedArray[C1,C2](x,getValueFun1,getValueFun2,compareFun1,compareFun2))
    }
  }

  implicit class KeyOneDimArray[K:ClassTag,C1,C2](rdd:RDD[(K,OneDimSortedArray[C1,C2])]) extends Serializable{

    def getMf:RDD[(K,Array[(C1,C2)])]={
      rdd.mapValues(array=>array.getMin)
    }

  }

  private def extractFields(l: Array[Any], r: Array[Any], indices1: Array[Int], indices2: Array[Int]): Array[Any] = {
    val buffer = Array.ofDim[Any](indices1.length + indices2.length)
    var current = 0
    for (i <- indices1) {
      buffer(current) = l(i)
      current += 1
    }

    for (i <- indices2) {
      buffer(current) = r(i)
      current += 1
    }

    buffer
  }

  private def binarySearchInDictionary[C1, C2](dict: Array[(C1,C2)], key: C1, func: (C1, C1) => Boolean): C2 = {
    var left = 0
    var right = dict.length
    while (left < right) {
      val mid = (left + right) / 2
      if (func(dict(mid)._1, key)) {
        left = mid + 1
      } else {
        right = mid
      }
    }

    dict(left - 1)._2
  }
}
