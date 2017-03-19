package org.hammerlab.pageant.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object WriteLines {
  def apply(dir: Path, fn: String, strs: Iterator[String], force: Boolean, conf: Configuration): Unit = {
    val path = new Path(dir, fn)
    val fs = path.getFileSystem(conf)
    if (!force && fs.exists(path)) {
      println(s"Skipping $path, already exists")
    } else {
      val os = fs.create(path)
      os.writeBytes(strs.mkString("", "\n", "\n"))
      os.close()
    }
  }
}
