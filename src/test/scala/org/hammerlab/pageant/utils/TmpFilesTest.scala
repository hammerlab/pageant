package org.hammerlab.pageant.utils

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.scalatest.Suite

import scala.collection.mutable.ArrayBuffer

trait TmpFilesTest extends BeforeAndAfterTest {
  self: Suite =>

  var tmpdirBefores = ArrayBuffer[File => Unit]()

  val dirsToDelete: ArrayBuffer[File] = ArrayBuffer()
  val filesToDelete: ArrayBuffer[File] = ArrayBuffer()

  def tmpPath(name: String = "tmp"): String = {
    tmpFile(name).toString
  }

  def tmpFile(name: String = "tmp"): File = {
    val f = File.createTempFile(name, ".tmp")
    //filesToDelete.append(f)
    f.deleteOnExit()
    f
  }

  def tmpDirPath(name: String = "tmp"): String = tmpDir(name).toString
  def tmpDir(name: String = "tmp"): File = {
    val file = new File(Files.createTempDirectory(name).toString)
    dirsToDelete.append(file)
    file
  }

  afters.append(() => {
    dirsToDelete.foreach(FileUtils.deleteDirectory)
  })
}
