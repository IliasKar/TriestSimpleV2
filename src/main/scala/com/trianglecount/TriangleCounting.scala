/**
  *     Triangle Counting in large graphs
  *     Triest implementation
  *     with spark streaming
  *
  */

// import java.util.logging.{Level, Logger}

package com.trianglecount


import java.time.LocalDateTime

import org.apache.logging.log4j.scala.Logging
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel._

class FileActor extends Actor {
  def receive = {
    case "startFiles" => {
      println("Start generating files")
      val fs_local : Boolean = true

      val pathToDataR = "hdfs://sp-cluster-1:9000/karalis/triangle/"
      val pathToDataL = "src/main/data/"
      var j = 0
      val fs = "com-youtube.txt"
      val rythmBatch = 3000
      val splitByLines = 40000
      val sf = new SplitFile()
      if (fs_local == true)
        j = (sf.splitFile(pathToDataL+fs, rythmBatch, splitByLines) )
      else
        j = (sf.splitFile(pathToDataR+fs, rythmBatch, splitByLines) )
    }
    case _       => println("error")
  }
}



object triangleCounting {
 // val loggerMy = Logger.getLogger(getClass.getName)


  // Define triangle case class
  case class Triangle (a: Long, b: Long, c: Long)
  case class localTriangleCount (a: Long, b: Long)


  def main(args: Array[String]) {

    /**
      *
      * ΜΕΤΡΗΤΕΣ
      * edgeTotalCounter : Long είναι όλες οι ακμές που έχουν διαβαστεί από το Dstream μέχρι στιγμής
      * edgeCurrentCounter : Long όλες οι ακμές που βρίσκονται ακόμα στο γράφο μετά από ενδεχόμενες διαγραφές
      * edgeLocalRDDCounter: Int μετράει όλες τις ακμές που επεξεργάζονται σε ένα RDD
      * edgeCounterInSample : Long μετράει όλες τις ακμές, οι οποίες έχουν ήδη εισαχθεί στο Sample
      * globalTriangleCounter : Long μετράει τα τρίγωνα τα οποί πραγματικά έχουν εντοπισθεί στο sample
      * globalTriangleEstimationAtCurrentRDD είναι ο αριθμός τριγώνων, ο οποίος υπολογίζεται μετά από κάθε
      *                 micro batch RDD με βάση τον τύπο υπολογισμού του Triest αλγόριθμου
      *
      */
    val sampleLength : Int = 400000  // Triest sample bucket
    val sampleLengthL : Long = 400000

    val fs_local = true
    val pathToSplitFilesR = "hdfs://sp-cluster-1:9000/karalis/triangle/input"
    val pathToSplitFilesL = "src/main/input"

    val pathToCheckpointR = "hdfs://sp-cluster-1:9000/karalis/triangle/resources"
    val pathToCheckpointL = "src/main/resources"

    var edgeLocalRDDCounter: Long = 0

    //    var d0 : Long = 0  // number of deleted edges outside the sample
//    var d1 : Long = 0 // number of deleted edges inside the sample
    println("***********************************************************************************************")
    println("***********************************************************************************************")
    println("Streaming demo application using Spark.")
    val timeStart = LocalDateTime.now()
    println("Current timestamp: " + timeStart)
 //   val splitByLines = 25
    val batchTime = 4
//    val windowSize = 4
//    val slideSize = 4
//    val windowDur = batchTime * windowSize
//    val slideDur = batchTime * slideSize


    val spark = SparkSession
      .builder
      .appName("TriangleCountStream")
      .master("local[4]")
      .getOrCreate()
/*
    val sparkR = SparkSession
      .builder
      .appName("TriangleCountStream")
      .getOrCreate()
*/

    val sc = spark.sparkContext

 //   if (fs_local == true) {
//      spark.conf.set("master", "local[4]")
    //  sparkL.conf.set("spark.sql.shuffle.partitions", 5)

  //  }

    Logger.getLogger("org").setLevel(Level.ERROR) // show only errors

//    import spark.implicits._

    val ssc = new StreamingContext(sc, Seconds(batchTime)) // Batch interval of 5 seconds

    var edgeTotalCounter : Long = 0
    //var edgeTotalCounter: LongAccumulator = sc.longAccumulator("countE")   // counter of total edges streamed until now

    var edgeCounterInSample : Long = 0
    var batchCounter: LongAccumulator = sc.longAccumulator("countB")   // counter of total edges streamed until now

    var globalTriangleCounter : Long = 0   // global triangle counter
    var globalTriangleEstimationAtCurrentRDD : Long = 0

    //


  //  loggerMy.error(s"This is for my logger")

    /**
      * sampleLength : Int ορίζει το μέγεθος του Sample
      * Sample: το κάθε φορά τυχαία επιλεγμένο σύνολο ακμών, το ποποίο επιλέγεται με
      *         τονα αλγόριθμο reservoir sampling
      */
    /**
      * Χρησιμοποιείται η συνάρτηση  splitFile για να σπάσει τα αρχικό αρχείο σε μικρότερα
      * τα οποία θα τροφοδοτήσουν το Stream ως micro batches
      * J είναι ο αριθμός σπασμένων αρχείων.
      */
    val system = ActorSystem("GenFilesSystem")
    val genActor = system.actorOf(Props[FileActor], name = "fileactor")
    genActor ! "startFiles"


    println("Gen files thread was started.")
    Thread.sleep(2000)
    println("Current timestamp  ssc start : " + LocalDateTime.now())

    /**
      * Stream τροφοδότηση:
      */

    var inputDir: String = ""
    if (fs_local == true)
        inputDir = pathToSplitFilesL
    else
        inputDir = pathToSplitFilesR
    val inputStream = ssc.textFileStream(inputDir)
      .map { line => line.split("\\s+") }.map(w => (w(0).toLong, w(1).toLong))

    //println("These are the process stream edges: ")

    // ρ: Τυχαίος αριθμός, ο οποίος χρησιμοποείται για την επιλογή ή όχι μίας ακμής
    //    από ένα micro batch στο σύνολο ακμών Sample

 //   var p = r.nextFloat()


    System.out.println("Streaming has started")



    /***
      * SampleRDD: είναι το RDD, το οποίο περιέχει ένα σύνολο ακμών δοκιμής. Με κάθε micro batch (RDD)
      * ο αλγόριθμος υπολογίζει τα τρίγωνα που σχηματίζονται από το σύνολο και τα προσμετρά, ώστε στο
      * τέλος της επεηεργασίας κάθε batch να γίνεται ο υπολογισμός του δυνητικού αριθμού τριγώνων
      * του γράφου μέχρι εκείνη τη στιγμή.
      * Το σύνολο αρχικά είναι άδειο και γεμίζει με δεδομάνα πό τη ροή. Όταν γεμίσει, με βάση τον αλγόριθμο
      * Triest επιλέγονται με κάποια πιθανοτητα ακμές για να εισέλθουν στο γράφο και να αντικαταστήσουν
      * ήδη υπάρχοντες.
      * edgesSampled : RDD[(Long, Long)]: Το RDD με τις επιλεγμένες ακμές, οι οποίες εισέρχονται στο Sample.
      *           Γενικά αντιπροσωπεύει τις ακμές για τις οποίες κάθε φορά υπολογίζουμε τον αριθμό τριγώνων
      *           που σχηματίζουν με το δείγμα.
      *
      */
    var sampleRDD: RDD[(Long,Long)] = ssc.sparkContext.emptyRDD[(Long,Long)]
    var edgesSampled : RDD[(Long, Long)] = ssc.sparkContext.emptyRDD[(Long, Long)]

    inputStream.foreachRDD(inputRdd => {
      println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
      println("Current timestamp  rdd start : " + LocalDateTime.now())

      var vertexNeighbors: RDD[(Long,List[Long])] = ssc.sparkContext.emptyRDD[(Long,List[Long])]
      var xi: Double = 1
      var sampleIndex: List[Long] = List()
      edgeLocalRDDCounter = 0

      var edgeTmpCount = inputRdd.count()
      var eTC = 0L
      if (!inputRdd.isEmpty()){
            /**
              * sampleIndex: είναι η λίστα στην οποία αποθηκεύονται τυχαία νούμερα από το ένα
              * μέχρι το SampleLength και τα οποία αντιστοιχούν στους δείκτες των ακμών, οι οποίες υπάρχουν στο
              * δείγμα και θα αντικατασταθούν από τις ακμές της λίστας toSampleRDD.
              */
 //           println("Number of partitions " + inputRdd.getNumPartitions)
            if (edgeTmpCount< 100) {
              inputRdd.foreach(println(_))
            }

            /**
              * Εάν το sampleRDD δεν έχει ακόμα γεμίσει με ακμές από τη ροή τότε
              * φορτώνεται σε αυτό το επόμενο micro batch.
              * Με κάθε ακμή αυξάνεται ο μετρητής ακμών που υπάρχουν στο δείγμα κατά ένα
              * Ο μετρητής αυξάνεται εδώ μία φορά για όλο το micro batch
              * Κατά τον ίδιο τρόπο αυξάνεται ο μετρητής των ακμών που έχουν συνολικά διαβαστεί
              * από τη ροή μέχρι τώρα.
              */
            if (edgeCounterInSample <= (sampleLength - edgeTmpCount)) {
              sampleRDD = sampleRDD.union(inputRdd).persist(MEMORY_AND_DISK)
              println("Processing batch RDD: " + batchCounter.value + " with edges: " + edgeTmpCount)
              edgeTotalCounter += edgeTmpCount
              println("Total number of edges: " +edgeTotalCounter)
              edgeCounterInSample = edgeCounterInSample + edgeTmpCount
              println("SampleRDD is not full and has:" + edgeCounterInSample + " number of edges.")
            }
            else if ((edgeCounterInSample > (sampleLength - edgeTmpCount))&&(edgeCounterInSample < sampleLength)) {
              val difCount: Long = edgeTmpCount-((edgeCounterInSample+edgeTmpCount)- sampleLength)
              val difRDD = sc.parallelize(inputRdd.take(difCount.toInt))
              sampleRDD = sampleRDD.union(difRDD).persist(MEMORY_AND_DISK)
              edgeCounterInSample = sampleLengthL
              println("Processing batch RDD: " + batchCounter.value + " with edges: " + edgeTmpCount)
              edgeTotalCounter += edgeTmpCount
              println("Total number of edges: " +edgeTotalCounter)
              println("SampleRDD is just full and has:" + edgeCounterInSample + " number of edges.")
            }
            else {

              /**
                * Εάν το δείγμα έχει γεμίζει με sampleLength γραμμές, τότε επιλέγονται με βάση πιθανότητες
                * κάποιες από τις ακμες του νέου micro batch για να αντικαταστήσουν τυχαία επιλεγμένες
                * ακμές του δείγματος.
                */

              println("SampleRDD is full and reservoir sampling is in process for input stream: " + batchCounter.value + ".")
           //   val processingRDD = inputRdd.collect()

              /**
                * toSampleRDD: είναι η λίστα με τις επιλεγμένες ακμές που θα αντικαταστήσουν ισάριθμες ακμές από το δείγμα
                */
              edgeTotalCounter += edgeTmpCount
//              println("inputRDD number of edges before filter: " + inputRdd.count())
   //           val fractionSample: Double = (1.0-(sampleLength.toDouble/edgeTotalCounter.toDouble))
   //           println("Fraction: " + fractionSample)
   //           val toSampleRDD = processingRDD.filter(x=> chooseX(x))
              val toSampleRDD: RDD[(Long, Long)] = inputRdd.sample(false, sampleLength.toDouble/edgeTotalCounter.toDouble ).cache()
              println("toSampleRDD is ready")
              val toCounterExclude = inputRdd.subtract(toSampleRDD)
  //            println("Accumulator of total edges is now: " + edgeTotalCounter)
              /**
                * Ο edgeLocalRDDCounter μετρητής απαριθμεί το πλήθος των ακμών που έχουν επιλέγει.
                */
 //             edgeLocalRDDCounter = toSampleRDD.size
              edgeLocalRDDCounter = toSampleRDD.count()
   //           println("This is the number of selected edges: " + edgeLocalRDDCounter + " for : " + toSampleRDD.count() + "!!!!!!!!!!!!!")
//              println("This is the number of selected edges: " + edgeLocalRDDCounter + " for : " + toSampleRDD.size + "!!!!!!!!!!!!!")
              //       toSampleRDD.foreach(println(_))

              if (edgeLocalRDDCounter == 0) {
                println("Warning: No edge was randomly selected for RDD. Head was taken instead.!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
              }

               /**
                *
                * Επιλέγονται με τυχαίο τρόπο uniformly οι δείκτες των ακμών οι οποίες θα αντικατασταθούν
                * με ακμές από το νέο micro batch. Επιλέγονται τόσες ακμές, όσες και οι ακμές που έχοθν επιλεγεί
                * από το νέο micro batch για να εισαχθούν στο σύνολο δείγματος.
                *
                */
              var z = 0
              while (z < edgeLocalRDDCounter) {
                val rn = scala.util.Random
                val sampleRddPosition: Long = rn.nextInt(sampleLength)
                if (!sampleIndex.contains(sampleRddPosition)) {
                  sampleIndex = sampleIndex :+ sampleRddPosition
                  // println("Z is : " + z)
                  z = z + 1
                }
              }

   //           println("Sample Indices to be replaced are following:" + sampleIndex.length)
 //             sampleIndex.foreach(println(_))


              /**
                * Αντικατάσταση των ακμων με βάση το δείκτη από το σύνολο δείγματος
                * με τις νέες ακμές.
                * tempRDDWithIndex: Πρώτα φιλτράρονται μόνο οι ακμές που παραμένουν στο δείγμα
                * και προστίθενται σε ένα προσωρινό RDD χωρίς τους δείκτες.
                * Το προσωρινό αυτό RDD γίνεται μετά το δείγμα και σε αυτό προστίθενται
                * όλες οι πειλεγμένες ακμες από το νεό micro batch
                */

              val tempRDDWithIndex: RDD[((Long, Long), Long)] = sampleRDD.zipWithIndex().filter(x => !sampleIndex.contains(x._2))
              // println(" Sample RDD has rows: " + sampleRDDWithIndex.count())
              /**
                * Όλοι οι κόμβοι Α από τις ακμές που θα αντικατασταθούν και για τους οποίους πρέπει να
                * αφαιρεθούν τα τρίγωνα, στα οποία συμμετέχουν από το συνολικο αριθμό τριγώνων τ.
                */
              //val tempRDDToBeReplaced = sampleRDDWithIndex.filter(x => sampleIndex.contains(x._2)).map(x => x._1._1).collect()
              // println(" Temp RDD has rows: " + tempRDDWithIndex.count())
              sampleRDD = tempRDDWithIndex.map(x => x._1)
     //         println(" Sample RDD without rows, which are replaced by new random rows: " + sampleRDD.count())

     //         println("This is the new sample RDD without the edges, which has been filtered out. ")
//              sampleRDD.foreach(println(_))


              /** *
                * edgesSampled RDD:  περιέχει όλες τις νέες ακμές που θα προστεθούν στο δείγμα
                */
              //edgesSampled = toSampleRDD
              //sampleRDD = sampleRDD.union(toSampleRDD)
  //            edgesSampled = ssc.sparkContext.parallelize(toSampleRDD)
              sampleRDD = sampleRDD.union(toSampleRDD).cache

       //       println("Sample RDD full and start counting triangles with number of rows: " + sampleRDD.count())
   //           sampleRDD.foreach(println(_))

            }
            println("Current timestamp  rdd middle : " + LocalDateTime.now())
            println("Current edge RDD count: " + inputRdd.count())






            val gV = sampleRDD.map(x=> x._1)
            val gEdges = sampleRDD.map(x=> Edge(x._1, x._2, ""))
            val sampleGraph = Graph.fromEdges(gEdges, MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut)
            val globalTriangleCounterVD = sampleGraph.triangleCount().vertices.map(_._2).reduce(_+_)
            globalTriangleCounter = globalTriangleCounterVD/3






/*
            vertexNeighbors = sampleRDD.union(sampleRDD.map(x => (x._2, x._1))).groupByKey().map(x => (x._1, x._2.toList)).reduceByKey(_ ++ _)

            val broadRdd = sc.broadcast(sampleRDD.collect().sortBy(x => (x._1, x._2)))

            val newTriangles: RDD[(Long, Long)] = vertexNeighbors.map(pivot => {

              var vertexTriangleInSampleHashM = mutable.HashMap[Long, Long]()

              for (i <- 0 until broadRdd.value.size) {

                // Get the endpoints of the edge
                val vertexA = broadRdd.value(i)._1
                val vertexB = broadRdd.value(i)._2

                //          println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
                //        println("pivot node: " + pivot._1)
                //      println("Checking edge: " + vertexA + "," + vertexB)

                // Check if both endpoints belong to the adjacency list of the the node

                //if (x._1 < vertexA && x._1 < vertexB) {
                if (pivot._2.contains(vertexA) && pivot._2.contains(vertexB)) {
                  //         println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
                  //      println("I found a new triangle!")

                  // Append the triangle to results
                  // var newTriangle = localTriangleCount(pivot._1,1)
                  var tempAccumulator: Long = 0
                  //       println("The corresponding node has: " + vertexTriangleInSampleHashM.getOrElse(pivot._1, 0))
                  tempAccumulator = vertexTriangleInSampleHashM.getOrElse(pivot._1, 0)
                  tempAccumulator += 1
                  //       println("The temp counter of triangles for node " + pivot._1 + " is: " + tempAccumulator)
                  vertexTriangleInSampleHashM.update(pivot._1, tempAccumulator)
                  //       println(" After update the value hashmap is " +vertexTriangleInSampleHashM.get(pivot._1))
                  //localTriangleC = newTriangle :: localTriangleC
                  // println(" This is the hashmap before going out of calculation")
                  // vertexTriangleInSampleHashM.foreach(println(_))

                }
                //}

                // println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")

              }

              //localTriangleC  // Return a list of new triangles for the pivot node
              vertexTriangleInSampleHashM
            }).flatMap {
              case (theMap) => theMap.map(e => (e._1, e._2))
            }

            // println(" This is the hashmap")
            //Thread.sleep(9000)


            // newTriangles.foreach(println(_))
            // globalTriangleCounter = newTriangles.map(x=> x._2).sum(_)
            globalTriangleCounter = newTriangles.collect().foldLeft(0.toLong) { case (sum, b) => sum + b._2 } / 3
*/
            val xi_in =  (edgeTotalCounter * (edgeTotalCounter - 1)).toDouble/(sampleLengthL * (sampleLengthL - 1) * (sampleLengthL - 2)).toDouble
            xi = xi_in * (edgeTotalCounter - 2)
//            xi = (edgeTotalCounter * (edgeTotalCounter - 1) * (edgeTotalCounter - 2)).toDouble / (sampleLengthL * (sampleLengthL - 1) * (sampleLengthL - 2)).toDouble
            // println("xi after division: " + xi)
            if (xi < 1)
              xi = 1
            globalTriangleEstimationAtCurrentRDD = (xi.round * globalTriangleCounter)
            println("Xi is at current RDD: " + xi)
            println("Total triangle counter at current RDD is: " + globalTriangleCounter)
            println("Estimation of total triangles in the Graph at current RDD is: " + globalTriangleEstimationAtCurrentRDD + "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            batchCounter.add(1L)

        println("Current timestamp rdd complete : " + LocalDateTime.now())

            /**
              * Σταματάει μετά την επεξεργασία όλων των αρχείων ροής
              */
      }
      else
        println("Batch was empty!!!!!!!!!!!!!")
    })

    ssc.start()
    ssc.awaitTermination()
    println("Current timestamp: " + LocalDateTime.now())
    val timeComplete = LocalDateTime.now()
    println("Current timestamp completion : " + timeComplete)
    println("Total time run: " + (timeComplete.compareTo(timeStart)))
    println("***********************************************************************************************")
    println("***********************************************************************************************")
  }
}