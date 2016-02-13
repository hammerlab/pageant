package org.hammerlab.pageant.utils

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.scalatest.Suite

import scala.collection.mutable.ArrayBuffer

trait TmpFilesTest extends BeforeAndAfterTest {
  self: Suite =>
  var dir: File = _

  var tmpdirBefores = ArrayBuffer[File => Unit]()

  befores.append(() => {
    dir = new File(Files.createTempDirectory("test").toString)
    tmpdirBefores.foreach(_(dir))
  })

  def tmpPath(name: String = ""): String = {
    tmpFile(name).toString
  }

  def tmpFile(name: String = ""): File = {
    File.createTempFile(name, ".tmp", dir)
  }

  def tmpDir(name: String = ""): String = {
    val file = tmpFile(name)
    file.mkdir()
    file.toString
  }

  afters.append(() => {
    FileUtils.deleteDirectory(dir)
  })
}
