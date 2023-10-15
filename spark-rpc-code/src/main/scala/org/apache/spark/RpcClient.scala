package org.apache.spark

import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object RpcClient {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
    val sparkSession = SparkSession.builder()
      .config(conf)
      .master("local[*]")
      .appName("spark-rpc-client")
      .getOrCreate()
    val sparkContext: SparkContext = sparkSession.sparkContext
    val sparkEnv: SparkEnv = sparkContext.env

    val rpcEnv: RpcEnv = RpcEnv
      .create(HelloRpcSettings.getName(), HelloRpcSettings.getHostname(), HelloRpcSettings.getPort(), conf,
        sparkEnv.securityManager, false)

    val endPointRef: RpcEndpointRef = rpcEnv
      .setupEndpointRef(RpcAddress(HelloRpcSettings.getHostname(), HelloRpcSettings.getPort()),
        HelloRpcSettings.getName())

    import scala.concurrent.ExecutionContext.Implicits.global
    endPointRef.send(SayHi("test send"))

    val future: Future[String] = endPointRef.ask[String](SayHi("test ask"))
    future.onComplete {
      case scala.util.Success(value) => println(s"Got the Ask result = $value")
      case scala.util.Failure(e) => println(s"Got the Ask error: $e")
    }
    Await.result(future, Duration.apply("30s"))

    val res = endPointRef.askSync[String](SayBye("test askSync"))
    println(res)

    sparkSession.stop()
  }

}
