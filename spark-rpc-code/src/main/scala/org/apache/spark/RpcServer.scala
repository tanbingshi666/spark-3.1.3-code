package org.apache.spark

import org.apache.spark.rpc.{RpcEndpoint, RpcEnv}
import org.apache.spark.sql.SparkSession

object RpcServer {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
    val sparkSession = SparkSession.builder()
      .config(conf)
      .master("local[*]")
      .appName("spark-rpc")
      .getOrCreate()
    val sparkContext: SparkContext = sparkSession.sparkContext

    val sparkEnv: SparkEnv = sparkContext.env

    val rpcEnv = RpcEnv.create(
      HelloRpcSettings.getName(),
      HelloRpcSettings.getHostname(),
      HelloRpcSettings.getHostname(),
      HelloRpcSettings.getPort(),
      conf,
      sparkEnv.securityManager,
      1,
      false)

    val helloEndpoint: RpcEndpoint = new HelloEndPoint(rpcEnv)
    rpcEnv.setupEndpoint(HelloRpcSettings.getName(), helloEndpoint)

    rpcEnv.awaitTermination()
  }

}
