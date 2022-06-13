package optics

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.concurrent.atomic.AtomicInteger

object Main extends App {

  val AA = new AtomicInteger(0)
  val AN = new AtomicInteger(0)
  val NA = new AtomicInteger(0)
  val NN = new AtomicInteger(0)

  val sparkSession = SparkSession.builder
    .master("local[*]")
    .appName("spark-app")
    .getOrCreate()

  val sparkContext = sparkSession.sparkContext
  sparkContext.setLogLevel("ERROR")
  println("*** Initialized Spark instance.")
  val streamingContext =
    new StreamingContext(sparkContext, batchDuration = Seconds(20))

  // Load and parse the data
  val data = sparkContext.textFile(
    "/Users/denis_savitsky/Downloads/train0.csv"
  )
  val parsedData =
    data
      .map(s => s.split(",").map(_.toDouble).dropRight(1))
      .cache()

  val model = new Optics(5, 1.2)
//  model.run(parsedData).foreach { p =>
////    println(s"INIT ID = ${p.id}")
//    println(
//      s"REACH = ${if (p.reachDis == Double.MaxValue) "MAXMAXMAXMAXMAXMAXMAXMAX"
//      else p.reachDis}"
//    )
////    println(s"ID = ${p.opticsId}")
////    println(s"coreDis = ${p.coreDis}")
//    println(s"IS CORE ${!p.notCore}")
//  }

  val trainResult = model
    .train(
      data
        .map(s => s.split(",").map(_.toDouble))
        .cache()
    )
//  trainResult
//    .foreach { p =>
//      println(s"INIT ID = ${p.id}")
//      println(
//        s"REACH = ${if (p.reachDis == Double.MaxValue) "MAXMAXMAXMAXMAXMAXMAXMAX"
//        else p.reachDis}"
//      )
////      println(s"ID = ${p.opticsId}")
////      println(s"coreDis = ${p.coreDis}")
////      println(s"IS CORE ${!p.notCore}")
//      println(s"IS ANOMALY: ${p.anomaly}")
//      if (p.anomaly && (p.reachDis == Double.MaxValue)) AA.incrementAndGet()
//      else if (p.anomaly) AN.incrementAndGet()
//      else if (!p.anomaly && (p.reachDis == Double.MaxValue))
//        NA.incrementAndGet()
//      else NN.incrementAndGet()
//    }

  val data1 = sparkContext.textFile(
    "/Users/denis_savitsky/Downloads/test0.csv"
  )
  val parsedData1 =
    data1
      .map(s => s.split(",").map(_.toDouble))
      .cache()
      .collect()

  parsedData1.take(2000).zipWithIndex.foreach { case (p, i) =>
    val res = model.update(
      Point(trainResult.length, p.dropRight(1))
    )
    println(s"ROUND $i")

//    println(s"ANOMALY: ${p.last == 1.0}")
//    println(res)

    if (p.last == 1.0 && (res == Anomaly)) AA.incrementAndGet()
    else if (p.last == 1.0) AN.incrementAndGet()
    else if (!(p.last == 1.0) && (res == Anomaly))
      NA.incrementAndGet()
    else NN.incrementAndGet()
  }

  println(s"AA = ${AA.get()}")
  println(s"AN = ${AN.get()}")
  println(s"NA = ${NA.get()}")
  println(s"NN = ${NN.get()}")

  val TP = AA.get().toDouble / (AA.get() + AN.get())
  val FP = AN.get().toDouble / (AA.get() + AN.get())
  val TN = NN.get().toDouble / (NN.get() + NA.get())
  val FN = NA.get().toDouble / (NN.get() + NA.get())

  println(s"TP = $TP")
  println(s"FP = $FP")
  println(s"TN = $TN")
  println(s"FN = $FN")
  println(s"RECAll = ${AA.get().toDouble / (AA.get() + AN.get())}")
  println(s"Precision = ${AA.get().toDouble / (AA.get() + NA.get())}")
  println(s"Specificity = ${NN.get().toDouble / (NN.get() + NA.get())}")
  println(s"FPR = ${NA.get().toDouble / (NN.get() + NA.get())}")
  println(
    s"F1 = ${2 * AA.get().toDouble / (2 * AA.get() + AN.get() + NA.get())}"
  )

  // AA = 951
  //AN = 541
  //NA = 2080
  //NN = 1428
  //TP = 0.6373994638069705
  //FP = 0.3626005361930295
  //TN = 0.4070695553021665
  //FN = 0.5929304446978335
  // F1 = 2 * 951 / (2 * 951 + 541 + 2080) = 0.421

//  RECAll = 0.2098408104196816
//  Precision = 0.4096045197740113
//  Specificity = 0.8403361344537815
//  FPR = 0.15966386554621848
//  F1 = 0.27751196172248804

}
