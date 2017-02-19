package org.hammerlab.pageant.coverage.two_sample

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.histogram.JointHistogram.Depth

trait CanDownSampleRDD[V] {
  def rdd: RDD[((Depth, Depth), V)]
  def filtersBroadcast: Broadcast[(Set[Depth], Set[Depth])]
  @transient lazy val filtered = filterDistribution(filtersBroadcast)

  private def filterDistribution(filtersBroadcast: Broadcast[(Set[Int], Set[Int])]): Array[((Depth, Depth), V)] =
    (for {
      ((d1, d2), value) ← rdd
      (d1Filter, d2Filter) = filtersBroadcast.value
      if d1Filter(d1) && d2Filter(d2)
    } yield
      (d1, d2) → value
    )
    .collect
    .sortBy(_._1)
}
