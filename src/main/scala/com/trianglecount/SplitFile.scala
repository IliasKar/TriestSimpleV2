
package com.trianglecount

// import java.util.logging.{Level, Logger}


import java.io._
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

  /**
    * Η συνάρτηση SplitFile, θα χωρίσει ένα μεγάλο σε πολλά μικρότερα
    * @param nameFileToSplit : το αρχείο που θα χωριστεί. Περιέχει όλες τις ακμές
    *                        ενός τεράστιου γράφου.
    * @return : μια σειρά από αρχεία, τα οποία προέρχονται απο τα αρχικό μοναδικό αρχείο
    *         και τα οποία θα βρίσκονται στο "input" directory
    *         Τα αρχεία θα χρησιμοποιηθούν σαν δεδομένα εισαγωγής στο
    *         Spark Streaming και θα δημιουργήσουν ένα DStream
    *         Each file has many lines as defined in spliByLines value
    *         Επίσης γυρνάει τον αριθμό των αρχείων που δημιουργήθηκαν.
    */

  class SplitFile() {

    val outputFilePrefix = "file_"
    val sparky = SparkSession
      .builder
      .appName("FileSplit")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR) // show only errors

    import sparky.implicits._

    val sc = sparky.sparkContext


    /**
      * Η συνάρτηση SplitFile, θα χωρίσει ένα μεγάλο σε πολλά μικρότερα
      *
      * @param nameFileToSplit : το αρχείο που θα χωριστεί. Περιέχει όλες τις ακμές
      *                        ενός τεράστιου γράφου.
      * @return : μια σειρά από αρχεία, τα οποία προέρχονται απο τα αρχικό μοναδικό αρχείο
      *         και τα οποία θα βρίσκονται στο "input" directory
      *         Τα αρχεία θα χρησιμοποιηθούν σαν δεδομένα εισαγωγής στο
      *         Spark Streaming και θα δημιουργήσουν ένα DStream
      *         Each file has many lines as defined in spliByLines value
      *         Επίσης γυρνάει τον αριθμό των αρχείων που δημιουργήθηκαν.
      */


    def splitFile(nameFileToSplit: String, rythmBatch: Int, splitByLines: Int): Int = {
      val fileToSplit = nameFileToSplit
      val textReader = sc.textFile(fileToSplit)
      val lines = textReader.map(line => line).collect().toIterator

      val sb = new StringBuilder
      var j = 0
      var i = 0
      var fs_local = true
      var inputDir = ""
      val pathToSplitFilesR = "hdfs://sp-cluster-1:9000/karalis/triangle/input/"
      val pathToSplitFilesL = "src/main/input/"
      var fs: FileSystem = FileSystem.getLocal(new Configuration())
      var outputFile = outputFilePrefix + j

      def writeToFile(sb: StringBuilder) = synchronized {
        val outFileStream = fs.create(new Path(inputDir + outputFile), true)
        val out = new OutputStreamWriter(outFileStream, "UTF-8")
        val os: BufferedWriter = new BufferedWriter(out)
        os.write(sb.toString())
        println(" Written in file " + outputFile)
        sb.clear()
        os.flush()
        os.close()
      }

      if (fs_local == true) {
        inputDir = pathToSplitFilesL
      }
      else {
        inputDir = pathToSplitFilesR

        val hdfsConf = new Configuration()
        hdfsConf.addResource(new Path("/etc/hadoop/core-site.xml"))
        hdfsConf.addResource(new Path("/etc/hadoop/hdfs-site.xml"))

        //  import org.apache.hadoop.fs.FileSystem
        fs = FileSystem.get(new URI("hdfs://sp-cluster-1:9000"), hdfsConf)
        //Create output stream to HDFS file
      }

      sb.clear()


      // os.flush( )
      //     os.close( )

      println("Streamwriter out set")


      for (line <- lines) {
        sb.append(line + "\n")

        i = i + 1
        if (i == splitByLines && (lines.hasNext)){
          writeToFile(sb)
          j = j + 1
          outputFile = outputFilePrefix + j
          Thread.sleep(rythmBatch)
          i = 0
        }
      }
      // out.write(sb.toString())
      writeToFile(sb)
      println(" Written in file " + outputFile)
 //     os.flush()
 //     os.close()
      sb.clear()
      //.close()
        j
      }
    }

