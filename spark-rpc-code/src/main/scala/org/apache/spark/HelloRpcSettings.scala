package org.apache.spark

object HelloRpcSettings {

  val rpcName = "hello-rpc-service"
  val port = 9527
  val hostname = "localhost"

  def getName() = {
    rpcName
  }

  def getPort(): Int = {
    port
  }

  def getHostname(): String = {
    hostname
  }

}
