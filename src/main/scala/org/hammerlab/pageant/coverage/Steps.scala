package org.hammerlab.pageant.coverage

import org.hammerlab.pageant.utils.RoundNumbers

object Steps {
  // Divide [0, maxDepth] into N geometrically-evenly-spaced steps (of size maxDepth^(1/100)),
  // including all sequential integers until those steps are separated by at least 1.
  def geometricEvenSteps(maxDepth: Int, N: Int = 100): Set[Int] = {
    import math.{exp, log, max, min}
    val logMaxDepth = log(maxDepth)

    val steps =
      (for {
        i ← 1 until N
      } yield
        min(maxDepth, max(i, exp((i - 1) * logMaxDepth / (N - 2)).toInt))
        ).toSet ++ Set(0)

    println(s"steps: ${steps.toList.sorted.mkString(",")}")

    steps
  }

  //  0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
  // 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
  // 20, 22, 24, 26, 28,
  // 30, 32, 34, 36, 38,
  // 40, 42, 44, 46, 48,
  // 50, 55,
  // 60, 65,
  // 70, 75,
  // 80, 85,
  // 90, 95,
  // repeat the [10, 95] portion, multiplied by powers of 10
  def roundNumberSteps(maxDepth: Int): Set[Int] = {
    (0 until 10).toSet ++
      RoundNumbers(
        (10 until 20) ++ (20 until 50 by 2) ++ (50 until 100 by 5),
        maxDepth,
        10
      ).toSet
  }
}
