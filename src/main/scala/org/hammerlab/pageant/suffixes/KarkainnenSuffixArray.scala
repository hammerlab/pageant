package org.hammerlab.pageant.suffixes

object KarkainnenSuffixArray extends SuffixArray {
  def radixPass(a: Array[Int], b: Array[Int], r: Seq[Int], n: Int, K: Int): Unit = {
    var c = Array.fill(K + 1)(0)
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

    val (n0, n1, n2) = ((n + 2) / 3, (n + 1) / 3, n / 3)
    val n02 = n0 + n2
    var s12 = Array.fill(n02 + 3)(0)
    var SA12 = Array.fill(n02 + 3)(0)

    var j = 0
    (0 until (n + n0 - n1)).foreach(i => {
      if (i % 3 != 0) {
        s12(j) = i
        j += 1
      }
    })

    radixPass(s12, SA12, s.view(2, n + 3), n02, K)
    radixPass(SA12, s12, s.view(1, n + 3), n02, K)
    radixPass(s12, SA12, s, n02, K)

    var name = 0
    var c = (-1, -1, -1)
    (0 until n02).foreach(i => {
      if (s(SA12(i)) != c._1 || s(SA12(i) + 1) != c._2 || s(SA12(i) + 2) != c._3) {
        name += 1
        c = (s(SA12(i)), s(SA12(i) + 1), s(SA12(i) + 2))
      }
      if (SA12(i) % 3 == 1) {
        s12(SA12(i) / 3) = name
      } else {
        s12(SA12(i) / 3 + n0) = name
      }
    })

    if (name < n02) {
      SA12 = make(s12, n02, name)
      (0 until n02).foreach(i => s12(SA12(i)) = i + 1)
    } else {
      (0 until n02).foreach(i => SA12(s12(i) - 1) = i)
    }

    var s0 = Array.fill(n0)(-1)
    var SA0 = Array.fill(n0)(-1)
    j = 0
    (0 until n02).foreach(i =>
      if (SA12(i) < n0) {
        s0(j) = 3 * SA12(i)
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
          SA12(t) * 3 + 1
        else
          (SA12(t) - n0) * 3 + 2

      // offset of current position in offset-0 array
      var i0 = SA0(p)

      val cmp =
        if (SA12(t) < n0)
          cmp2(
            (s(i12), s12(SA12(t) + n0)),
            (s(i0), s12(i0 / 3))
          )
        else
          cmp3(
            (s(i12), s(i12 + 1), s12(SA12(t) - n0 + 1)),
            (s(i0), s(i0 + 1), s12(i0 / 3 + n0))
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
                SA12(t) * 3 + 1
              else
                (SA12(t) - n0) * 3 + 2
            SA(k) = i12
            k += 1
            t += 1
          }
        }
      }
    }
    SA
  }
}

