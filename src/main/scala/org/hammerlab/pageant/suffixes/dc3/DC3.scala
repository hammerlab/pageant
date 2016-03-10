package org.hammerlab.pageant.suffixes.dc3

import scala.collection.mutable.ArrayBuffer

/*
  TODO:
  - Vector vs. Arrays, appending
  - parameterize zero-handling
 */

object DC3 {
  def radixPass(a: Seq[Int], b: Array[Int], r: Seq[Int], n: Int, K: Int, stableZeros: Boolean = true): Unit = {
    var c = Array.fill(K + 1)(0)
    val zeros: ArrayBuffer[Int] = ArrayBuffer()
    (0 until n).foreach(i => {
      val ai = a(i)
      val rv = r(ai)
      if (rv == 0) {
        zeros.append(ai)
      }
      c(rv) += 1
    })
    var sum = 0
    c = c.map(i => {
      val t = sum
      sum += i
      t
    })
    (0 until n).foreach(i => {
      val ai = a(i)
      val rv = r(ai)
      b(c(rv)) = ai
      c(rv) += 1
    })
    if (stableZeros && zeros.length > 1) {
      var d = Array.fill(r.length)(0)
      zeros.foreach(ai => d(ai) = ai)
      radixPass(zeros, b, d, zeros.length, r.length, stableZeros = false)
    }
  }

  def cmp2(t1: (Int, Int, Int), t2: (Int, Int, Int)): Boolean = {
    if (t1._1 < t2._1)
      true
    else if (t1._1 == t2._1)
      if (t1._1 == 0)
        t1._3 < t2._3
      else
        t1._2 <= t2._2
    else
      false
  }

  def cmp3(t1: (Int, Int, Int, Int), t2: (Int, Int, Int, Int)): Boolean = {
    if (t1._1 < t2._1)
      true
    else if (t1._1 == t2._1)
      if (t1._1 == 0)
        t1._4 < t2._4
      else if (t1._2 < t2._2)
        true
      else if (t1._2 == t2._2)
        if (t1._2 == 0)
          t1._4 < t2._4
        else
          t1._3 <= t2._3
      else
        false
    else
      false
  }

  def make(s: Array[Int], K: Int): Array[Int] = {
    // We always need at least two zeros of padding; if the size is 1%3, then the [12]%3-array's final 1%3 element, in
    // the middle of the array, will have no zeros in the triple that it anchors, raising the possibility that a suffix
    // of the [12]%3 array could be construed to carry through from the 1%3 side to the 2%3 side.
    //
    // As a result, for the length≣1%3 case, we add a virtual final 1%3 element anchoring a triple (0,0,0), perform all
    // sorting with it incorporated, and then filter it out in the end while building the final result.
    val padding =
      Array.fill(
        if (s.length % 3 > 0) 3 else 2
      )(0)

    make(
      s ++ padding,
      s.length,
      K,
      treatZerosAsSentinels = true
    )
  }

  def make(s: Array[Int], n: Int, K: Int, treatZerosAsSentinels: Boolean): Array[Int] = {
    if (n == 0) return Array()
    if (n == 1) return Array(0)

    val (n0, n1, n2) = ((n + 2) / 3, (n + 1) / 3, (n + 1) / 3)
    val n02 = n0 + n2

    // The [12]%3 indices of s will be placed in here, replaced with "name"s, and sent to the recursive make() call.
    // The latter step requires zero-padding as described in the make() wrapper above: 3 zeros if the length is 1%3,
    // 2 zeros otherwise.
    var s12 = Array.fill(n02 + (if (n02 % 3 > 0) 3 else 2))(0)

    // This will eventually store the suffix array of the [12]%3 elements of s.
    var SA12 = Array.fill(n02)(0)

    // Populate s12 with all [12]%3 indices in s, e.g. [1, 2, 4, 5, 7, 8, ...)
    var j = 0
    (0 until (n + (if (n % 3 > 0) 1 else 0))).foreach(i => {
      if (i % 3 != 0) {
        s12(j) = i
        j += 1
      }
    })

    // These radix passes have the net effect of sorting the [12]%3 s-indices by the s-triplets that they anchor.

    radixPass(s12, SA12, s.view(2, n + 3), n02, K, stableZeros = treatZerosAsSentinels)
    // Now SA12 contains the elements of s12 above, sorted by the elements 2 places ahead of them in s.

    radixPass(SA12, s12, s.view(1, n + 3), n02, K, stableZeros = treatZerosAsSentinels)
    // Now the torch has been passed back to s12, which has the [12]%3 elements sorted by the elements 1- and 2- places
    // ahead of them in s.

    radixPass(s12, SA12, s, n02, K, stableZeros = treatZerosAsSentinels)

    // Finally, SA12 is the [12]%3-indices of s, sorted by the triplets that they anchor.

    // Next we iterate in ascending order through the sorted triplets of s (stored in SA12 per the last step), replacing
    // them with "names": unique, increasing identifiers that preserve the triplets' relative order.
    //
    // Additionally, we rearrange the triplets (or rather the "name"s they have been replaced with) so that all 1%3
    // s-indices comprise the first n0 entries of s12 and all 2%3 indices are the latter n2.
    //
    // Note that all zeros are considered lexicographically distinct from one another, and the names start from 1
    // because multiple triplets assigned the same name should be treated as lexicographically equal when we recurse.
    var name = 0
    var c = (-1, -1, -1)
    (0 until n02).foreach(i => {
      val si = SA12(i)
      if (s(si) != c._1 || s(si + 1) != c._2 || s(si + 2) != c._3 || c._1 == 0 || c._2 == 0 || c._3 == 0) {
        name += 1
        c = (s(si), s(si + 1), s(si + 2))
      }
      if (si % 3 == 1) {
        s12(si / 3) = name
      } else {
        s12(si / 3 + n0) = name
      }
    })

    if (name < n02) {
      // If the maximum assigned "name" is less than the number of [12]%3 elements to which "name"s were assigned, then
      // there must have been at least one duplicate name (and originating triplet). In this case, we recurse on the
      // "names" array s12.
      //
      // Substrings of the names array in the front section (representing 1%3s) map directly to substrings of the
      // original string s; likewise substrings in the back (2%3) section.
      //
      // Comparing two suffixes where one crossed the 1%3/2%3 border would lead to erroneous sorting, as in the original
      // s the final 1%3 triplet is not succeeded by the first 2%3 triplet. This is prevented from happening by the
      // existence of at least one zero in the final 1%3 triplet (enforced by an extra 0 of padding and the inclusion of
      // a fake (0,0,0) triplet in the case that n≣1%3. The result is that no two suffixes can appear to be
      // lexicographically "tied" across the 1%3/2%3 border.
      SA12 = make(s12, n02, name, treatZerosAsSentinels = false)

      // Having obtained the full and correct [12]%3 suffix-array in SA12, we rewrite the s12 "names" array, now with no
      // duplicate names. This is basically the same as inverting teh suffix-array SA12, but with all entries increased
      // by 1 to avoid having risked multiple triplets being "named" 0 and processed as sentinels inappropriately.
      (0 until n02).foreach(i => s12(SA12(i)) = i + 1)
    } else {
      // If the names assigned are all unique, then they correspond to the lexicographic ranking of all suffixes, and
      // inverting them (again modulo s12's offset-by-1 discussed above) gives the suffix array of the [12]%3 s-indices.
      (0 until n02).foreach(i => SA12(s12(i) - 1) = i)
    }

    // At this point, SA12 is the suffix array of s12, and thus a permutation of [0, n02).
    // s12 is its inverse, with all values increased by 1 as an artifact of the sorting and sentinel-handling above.
    //
    // Next we will build a suffix-array of the 0%3 indices by utilizing the fact that they can be simply sorted amongst
    // themselves, with ties broken by the SA12 ranks of their 1%3 successors.

    // First, create s0: the 0%3 indices sorted by successor-1%3-suffixes' SA12-rank.
    // Note the entries in s0 comprise all multiples of 3 in [0, n).
    var s0 = Array.fill(n0)(-1)
    j = 0
    (0 until n02).foreach(i =>
      if (SA12(i) < n0) {
        s0(j) = 3 * SA12(i)
        j += 1
      }
    )

    // Next, create SA0 by stably sorting s0 by the actual elements in s at 0%3 indices, ties broken by the above
    // 1%3-successor-sort.
    var SA0 = Array.fill(n0)(-1)
    radixPass(s0, SA0, s, n0, K, stableZeros = treatZerosAsSentinels)

    // SA0 is now the 0%3 indices in correct order (according to the suffixes they anchor).

    var p = 0
    var t = 0
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
            (s(i12), s12(SA12(t) + n0), i12),
            (s(i0), s12(i0 / 3), i0)
          )
        else
          cmp3(
            (s(i12), s(i12 + 1), s12(SA12(t) - n0 + 1), i12),
            (s(i0), s(i0 + 1), s12(i0 / 3 + n0), i0)
          )

      if (cmp) {
        if (i12 != n) {
          SA(k) = i12
          k += 1
        }
        t += 1
        if (t == n02) {
          while (p < n0 && k < n) {
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
          while (t < n02 && k < n) {
            i12 =
              if (SA12(t) < n0)
                SA12(t) * 3 + 1
              else
                (SA12(t) - n0) * 3 + 2
            if (i12 != n) {
              SA(k) = i12
              k += 1
            }
            t += 1
          }
        }
      }
    }
    SA
  }
}

