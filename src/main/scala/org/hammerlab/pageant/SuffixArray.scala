package org.hammerlab.pageant

object SuffixArray {
  def radixPass(a: Array[Int], b: Array[Int], r: Seq[Int], n: Int, K: Int): Unit = {
    var c = Array.fill(K+1)(0)
    (0 until n).foreach(i => c(r(a(i))) += 1)
    var sum = 0
    c = c.map(i => {
      val t = sum
      sum += i
      t
    })
    (0 until n).foreach(i => {
      b(c(r(a(i)))) = a(i)
      c(r(a(i))) += 1
    })
  }

  def cmp2(t1: (Int, Int), t2: (Int, Int)): Boolean = {
    t1._1 < t2._1 || (t1._1 == t2._1 && t1._2 <= t2._2)
  }

  def cmp3(t1: (Int, Int, Int), t2: (Int, Int, Int)): Boolean = {
    t1._1 < t2._1 ||
      (t1._1 == t2._1 &&
        (t1._2 < t2._2 ||
          (t1._2 == t2._2 && t1._3 <= t2._3)
        )
      )
  }

  def make(s: Array[Int], K: Int): Array[Int] = make(s.map(_ + 1) ++ Array(0, 0, 0), s.size, K)
  def make(s: Array[Int], n: Int, K: Int): Array[Int] = {
    if (n == 0) return Array()
    if (n == 1) return Array(0)

    val (n0, n1, n2) = ((n+2)/3, (n+1)/3, n/3)
    val n02 = n0 + n2
    var s12 = Array.fill(n02 + 3)(0)
    var SA12 = Array.fill(n02 + 3)(0)

    var j = 0
    (0 until (n+n0-n1)).foreach(i => {
      if (i % 3 != 0) {
        s12(j) = i
        j += 1
      }
    })

    radixPass(s12, SA12, s.view(2, n+3), n02, K)
    radixPass(SA12, s12, s.view(1, n+3), n02, K)
    radixPass(s12, SA12, s, n02, K)

    var name = 0
    var c = (-1, -1, -1)
    (0 until n02).foreach(i => {
      if (s(SA12(i)) != c._1 || s(SA12(i)+1) != c._2 || s(SA12(i)+2) != c._3) {
        name += 1
        c = (s(SA12(i)), s(SA12(i)+1), s(SA12(i)+2))
      }
      if (SA12(i) % 3 == 1) {
        s12(SA12(i)/3) = name
      } else {
        s12(SA12(i)/3 + n0) = name
      }
    })

    if (name < n02) {
      SA12 = SuffixArray.make(s12, n02, name)
      (0 until n02).foreach(i => s12(SA12(i)) = i + 1)
    } else {
      (0 until n02).foreach(i => SA12(s12(i) - 1) = i)
    }

    var s0 = Array.fill(n0)(-1)
    var SA0 = Array.fill(n0)(-1)
    j = 0
    (0 until n02).foreach(i =>
      if (SA12(i) < n0) {
        s0(j) = 3*SA12(i)
        j += 1
      }
    )
    radixPass(s0, SA0, s, n0, K)

    var p = 0
    var t = n0 - n1
    var k = 0
    var SA = Array.fill(n)(-1)
    while (k < n) {

      // offset of current position in offset-12 array
      var i12 =
        if (SA12(t) < n0)
          SA12(t)*3 + 1
        else
          (SA12(t) - n0)*3 + 2

      // offset of current position in offset-0 array
      var i0 = SA0(p)

      val cmp =
        if (SA12(t) < n0)
          cmp2(
            (s(i12), s12(SA12(t) + n0)),
            (s(i0), s12(i0/3))
          )
        else
          cmp3(
            (s(i12), s(i12 + 1), s12(SA12(t) - n0 + 1)),
            (s(i0), s(i0 + 1), s12(i0/3 + n0))
          )

      if (cmp) {
        SA(k) = i12
        t += 1
        k += 1
        if (t == n02) {
          while (p < n0) {
            SA(k) = SA0(p)
            k += 1
            p += 1
          }
        }
      } else {
        SA(k) = i0
        p += 1
        k += 1
        if (p == n0) {
          while (t < n02) {
            i12 =
              if (SA12(t) < n0)
                SA12(t)*3 + 1
              else
                (SA12(t) - n0)*3 + 2
            SA(k) = i12
            k += 1
            t += 1
          }
        }
      }
    }
    SA
  }

  def reverse(a: Array[Int]): Array[Int] = {
    var ret = Array.fill(a.size)(-1)
    var n = 0
    for {
      (i, j) <- a.zipWithIndex
    } {
      if (i >= a.size) {
        throw new Exception(s"Reversing error: ${a.mkString(",")}")
      }
      if (ret(i) == -1) n+= 1
      ret(i) = j
    }
    if (n < a.size) {
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
    var idxs = Array.fill(a.size)(-1)
    for {
      (t, i) <- a.zipWithIndex
      idx = fn(t)
    } {
      idxs(i) = bucketIdxs(idx)
    }
    idxs
  }

  def apply(a: Array[Int], n: Int): Array[Int] = {
    if (a.isEmpty) return Array[Int]()
    if (a.size == 1) return Array(0)

    val paddedArray = a.map(_ + 1) ++ ((a.size % 3) match {
      case 0 => Array(0, 0, 0, 0)
      case 1 => Array(0, 0, 0)
      case 2 => Array(0, 0, 0)
    })
    val N = n + 1

    val a12 = (0 until paddedArray.size - 2).toArray.filter(_ % 3 != 0)

    val n1 = (a12.size + 1)/2

    val arr =
      radixSort[(Int, Int, Int)](
        (for {
          i <- a12
        } yield {
          (paddedArray(i), paddedArray(i+1), paddedArray(i+2))
        }),
        (t) => N*N + N + N*N*t._1 + N*t._2 + t._3,
        N*N*N + N*N + N
      )

    var arrMax = arr.max
    var dupes = arrMax + 1 != arr.size

    val tthArr =
      if (dupes) {
        val segd = ((0 until arr.size by 2) ++ (1 until arr.size by 2)).map(i => arr(i)).toArray
        val rsa = SuffixArray(segd, segd.size)
        val uninterleaved =
          rsa.indices.toArray.map(i =>
            if (rsa(i) < n1)
              2*rsa(i)
            else
              2*(rsa(i)-n1) + 1
          )

        if (uninterleaved(0) != uninterleaved.size - 1) {
          throw new Exception(s"Expected first pos to point to end: ${uninterleaved.mkString(",")}")
        }
        reverse(uninterleaved.drop(1))
      } else {
        arr.dropRight(1).map(_ - 1)
      }
    val tthRev = reverse(tthArr)

    val paddedTthArr =
      if (a.size % 3 == 1)
        tthArr.map(_ + 1) :+ 0
      else
        tthArr

    val fthArr =
      radixSort[(Int, Int)](
        for {
          i <- (0 until (a.size + 2)/ 3).toArray
        } yield {
          (a(3*i), paddedTthArr(2*i))
        },
        (t) => paddedTthArr.size * t._1 + t._2,
        n * paddedTthArr.size
      )

    val fthRev = reverse(fthArr)

    def cmp01(i: Int, j: Int): Boolean = {
      if (i > a.size || j > a.size) {
        throw new Exception(s"bad indices: $i $j")
      }
      val r = if (j == a.size)
        false
      else if (i == a.size)
        true
      else if (a(i) == a(j)) {
        cmp12(i+1, j+1)
      } else {
        a(i) < a(j)
      }
      r
    }

    def cmp02(i: Int, j: Int): Boolean = {
      if (i > a.size || j > a.size) {
        throw new Exception(s"bad indices: $i $j")
      }
      val r = if (j == a.size)
        false
      else if (i == a.size)
        true
      else if (a(i) == a(j)) {
        !cmp01(j+1, i+1)
      } else {
        a(i) < a(j)
      }
      r
    }

    def cmp12(i: Int, j: Int): Boolean = {
      if (i > a.size || j > a.size) {
        throw new Exception(s"bad indices: $i $j")
      }
      val r = if (j == a.size)
        false
      else if (i == a.size)
        true
      else
        tthArr(i/3*2) < tthArr(j/3*2 + 1)
      r
    }

    var i = 0
    var j = 0

    var curIdx = 0
    var sa = Array.fill(a.size)(-1)
    while (i < fthArr.size && j < tthArr.size) {
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
    while (i < fthArr.size) {
      val ii = 3*fthRev(i)
      sa(curIdx) = ii
      i += 1
      curIdx += 1
    }
    while (j < tthArr.size) {
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
