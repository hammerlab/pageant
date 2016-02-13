package org.hammerlab.pageant.utils

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.scalatest.Suite

trait TmpFilesTest extends BeforeAndAfterTest {
  self: Suite =>
  var dir: File = _

  befores.append(() => {
    dir = new File(Files.createTempDirectory("test").toString)
  })

  def tmpFile(name: String = ""): String = {
    val file = File.createTempFile(name, ".tmp", dir)
    file.toString
  }
  afters.append(() => {
    FileUtils.deleteDirectory(dir)
  })
}
