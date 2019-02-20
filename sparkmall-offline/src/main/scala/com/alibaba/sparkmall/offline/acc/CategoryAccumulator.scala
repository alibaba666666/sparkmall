package com.alibaba.sparkmall.offline.acc

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable


class CategoryAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Long]] {

    var categoryMap:mutable.HashMap[String,Long]=new mutable.HashMap[String,Long]()

    override def isZero: Boolean = categoryMap.isEmpty

    override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
        new CategoryAccumulator()
    }

    override def reset(): Unit = {
        categoryMap.clear()
    }

    override def add(key: String): Unit = {
        categoryMap(key) = categoryMap.getOrElse(key,0L) + 1L
    }

    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
        val otherMap: mutable.HashMap[String,Long] = other.value
        categoryMap = categoryMap.foldLeft(otherMap) {case (otherMap,(key,count)) =>
                otherMap(key) = otherMap.getOrElse(key,0L) + count
                otherMap
        }
    }

    override def value: mutable.HashMap[String, Long] = {
        categoryMap
    }
}
