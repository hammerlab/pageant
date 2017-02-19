package org.hammerlab.pageant.coverage.two_sample

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.coverage
import org.hammerlab.pageant.histogram.JointHistogram.Depth
import spire.algebra.Monoid

case class PDF[C: Monoid](rdd: RDD[((Depth, Depth), C)],
                          filtersBroadcast: Broadcast[(Set[Depth], Set[Depth])],
                          maxDepth1: Depth,
                          maxDepth2: Depth)
  extends coverage.PDF[C]
    with CanDownSampleRDD[C]

case class CDF[C: Monoid](rdd: RDD[((Depth, Depth), C)],
                          filtersBroadcast: Broadcast[(Set[Depth], Set[Depth])])
  extends coverage.CDF[C]
    with CanDownSampleRDD[C]
