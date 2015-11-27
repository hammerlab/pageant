package org.hammerlab.pageant.suffixes

object WIPSuffixArray extends SuffixArray {
  def reverse(a: Array[Int]): Array[Int] = {
    var ret = Array.fill(a.length)(-1)
    var n = 0
    for {
      (i, j) <- a.zipWithIndex
    } {
      if (i >= a.length) {
        throw new Exception(s"Reversing error: ${a.mkString(",")}")
      }
      if (ret(i) == -1) n+= 1
      ret(i) = j
    }
    if (n < a.length) {
      throw new Exception(s"Reversing error: ${a.mkString(",")}")
    }
    ret
  }

  def radixSort[T](a: Array[T], fn: T => Int, max: Int): Array[Int] = {
    var buckets = Array.fill(max)(0)
    for {
      t <- a
      idx = fn(t)
    } {
      if (idx >= max) {
        throw new Exception(s"idx $idx >= $max for $t, arr: ${a.mkString(",")}")
      }
      buckets(idx) += 1
    }
    var bucketIdxs = Array.fill(max)(-1)
    var curIdx = 0
    for {
      (c, i) <- buckets.zipWithIndex
      if c > 0
    } {
      bucketIdxs(i) = curIdx
      curIdx += 1
    }
    var idxs = Array.fill(a.length)(-1)
    for {
      (t, i) <- a.zipWithIndex
      idx = fn(t)
    } {
      idxs(i) = bucketIdxs(idx)
    }
    idxs
  }

  override def make(a: Array[Int], K: Int): Array[Int] = {
    if (a.isEmpty) return Array[Int]()
    if (a.length == 1) return Array(0)

    val paddedArray = a.map(_ + 1) ++ (a.length % 3 match {
      case 0 => Array(0, 0, 0, 0)
      case 1 => Array(0, 0, 0)
      case 2 => Array(0, 0, 0)
    })
    val N = K + 1

    val a12 = (0 until paddedArray.length - 2).toArray.filter(_ % 3 != 0)

    val n1 = (a12.length + 1) /2

    val arr =
      radixSort[(Int, Int, Int)](
        for {
          i <- a12
        } yield {
          (paddedArray(i), paddedArray(i+1), paddedArray(i+2))
        },
        (t) => N*N + N + N*N*t._1 + N*t._2 + t._3,
        N*N*N + N*N + N
      )

    var arrMax = arr.max
    var dupes = arrMax + 1 != arr.length

    val tthArr =
      if (dupes) {
        val segd = ((arr.indices by 2) ++ (1 until arr.length by 2)).map(i => arr(i)).toArray
        val rsa = make(segd, segd.length)
        val uninterleaved =
          rsa.indices.toArray.map(i =>
            if (rsa(i) < n1)
              2*rsa(i)
            else
              2*(rsa(i)-n1) + 1
          )

        if (uninterleaved(0) != uninterleaved.length - 1) {
          throw new Exception(s"Expected first pos to point to end: ${uninterleaved.mkString(",")}")
        }
        reverse(uninterleaved.drop(1))
      } else {
        arr.dropRight(1).map(_ - 1)
      }
    val tthRev = reverse(tthArr)

    val paddedTthArr =
      if (a.length % 3 == 1)
        tthArr.map(_ + 1) :+ 0
      else
        tthArr

    val fthArr =
      radixSort[(Int, Int)](
        for {
          i <- (0 until (a.length + 2) / 3).toArray
        } yield {
          (a(3*i), paddedTthArr(2*i))
        },
        (t) => paddedTthArr.length * t._1 + t._2,
        K * paddedTthArr.length
      )

    val fthRev = reverse(fthArr)

    def cmp01(i: Int, j: Int): Boolean = {
      if (i > a.length || j > a.length) {
        throw new Exception(s"bad indices: $i $j")
      }
      val r = if (j == a.length)
        false
      else if (i == a.length)
        true
      else if (a(i) == a(j)) {
        cmp12(i+1, j+1)
      } else {
        a(i) < a(j)
      }
      r
    }

    def cmp02(i: Int, j: Int): Boolean = {
      if (i > a.length || j > a.length) {
        throw new Exception(s"bad indices: $i $j")
      }
      val r = if (j == a.length)
        false
      else if (i == a.length)
        true
      else if (a(i) == a(j)) {
        !cmp01(j+1, i+1)
      } else {
        a(i) < a(j)
      }
      r
    }

    def cmp12(i: Int, j: Int): Boolean = {
      if (i > a.length || j > a.length) {
        throw new Exception(s"bad indices: $i $j")
      }
      val r = if (j == a.length)
        false
      else if (i == a.length)
        true
      else
        tthArr(i/3*2) < tthArr(j/3*2 + 1)
      r
    }

    var i = 0
    var j = 0

    var curIdx = 0
    var sa = Array.fill(a.length)(-1)
    while (i < fthArr.length && j < tthArr.length) {
      val ji = tthRev(j)
      val (cmp, jii) = if (ji % 2 == 0) {
        (cmp01 _, ji/2*3 + 1)
      } else {
        (cmp02 _, ji/2*3 + 2)
      }
      val ii = 3*fthRev(i)
      if (cmp(ii, jii)) {
        sa(curIdx) = ii
        i += 1
      } else {
        sa(curIdx) = jii
        j += 1
      }
      curIdx += 1
    }
    while (i < fthArr.length) {
      val ii = 3*fthRev(i)
      sa(curIdx) = ii
      i += 1
      curIdx += 1
    }
    while (j < tthArr.length) {
      val ji = tthRev(j)
      val jii = if (ji % 2 == 0) {
        ji/2*3 + 1
      } else {
        ji/2*3 + 2
      }
      sa(curIdx) = jii
      j += 1
      curIdx += 1
    }
    sa
  }
}
