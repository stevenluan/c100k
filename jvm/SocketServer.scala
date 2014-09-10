import java.io.EOFException
import java.net.{SocketException, InetSocketAddress}
import java.nio._
import java.nio.channels.{SocketChannel, SelectionKey, ServerSocketChannel, Selector}
import java.sql.Time
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}
import java.util.{TimerTask, Timer, Date}

/**
 * Created by hluan on 9/5/14.
 */
class SocketServer(val host:String, val ports:Array[Int], val numProcessor:Int
                   , val sendBufferSize: Int,
                   val recvBufferSize: Int ,
                   val maxRequestSize: Int, val metricsEmitIntervalInSec:Int) {
  private val processors = new Array[Processor](numProcessor);
  @volatile private var acceptor:Acceptor = null;
  def startUp() = {
    val time = System.currentTimeMillis
    val metrics = new Metrics(metricsEmitIntervalInSec * 1000)
    for(i <- 0 until numProcessor){
      processors(i) = new Processor(i, time, maxRequestSize, metrics);
      processors(i).start();
    }
    this.acceptor = new Acceptor(host, ports, processors, recvBufferSize, sendBufferSize, metrics);
    this.acceptor.start();
    info("server started");
  }

  def shutDown() = {
    info("server shutting down")
    if(acceptor!=null) acceptor.shutdown()
    for(i <- 0 until numProcessor){
      processors(i).shutdown();
    }
  }

  private def info(line:String) = println(line)
  private class ServerThread extends Thread {
    protected val selector = Selector.open();
    private val startupLatch = new CountDownLatch(1)
    private val shutdownLatch = new CountDownLatch(1)
    private val alive = new AtomicBoolean(false)

    /**
     * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
     */
    def shutdown(): Unit = {
      alive.set(false)
      selector.wakeup()
      shutdownLatch.await
    }

    /**
     * Wait for the thread to completely start up
     */
    def awaitStartup(): Unit = startupLatch.await

    /**
     * Record that the thread startup is complete
     */
    protected def startupComplete() = {
      alive.set(true)
      startupLatch.countDown
    }

    /**
     * Record that the thread shutdown is complete
     */
    protected def shutdownComplete() = shutdownLatch.countDown

    /**
     * Is the server still running?
     */
    protected def isRunning = alive.get

    /**
     * Wakeup the thread for selection.
     */
    def wakeup() = selector.wakeup()
  }

  private class Acceptor(val host:String, val ports:Array[Int], val processors:Array[Processor]
                         , val recvBufferSize:Int, val sendBufferSize:Int, val metrics:Metrics) extends ServerThread {
    val serverChannels = openServerSocket(host, ports)
    override def run() = {
      for(serverChannel <- serverChannels){
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
      }

      startupComplete()
      var currentProcessor = 0
      while(isRunning) {
        val ready = selector.select(500)
        if(ready > 0) {
          val keys = selector.selectedKeys()
          val iter = keys.iterator()
          while(iter.hasNext && isRunning) {
            var key: SelectionKey = null

            try {
              key = iter.next
              iter.remove()
              if(key.isAcceptable)
                accept(key, processors(currentProcessor))
              else
                throw new IllegalStateException("Unrecognized key state for acceptor thread.")
              // round robin to the next processor thread
              currentProcessor = (currentProcessor + 1) % processors.length
            } catch {
              case e: Throwable => new Error("Error in acceptor", e)
            }
          }
        }
      }
      info("Closing server socket and selector.")
      for(serverChannel <- serverChannels){
        serverChannel.close()
      }
      selector.close()
      shutdownComplete()
    }
    def accept(key:SelectionKey, processor:Processor) = {

      val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
      serverSocketChannel.socket().setReceiveBufferSize(recvBufferSize)

      val socketChannel = serverSocketChannel.accept()
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setSendBufferSize(sendBufferSize)

//      info("Accepted connection from %s on %s. sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
//        .format(socketChannel.socket.getInetAddress, socketChannel.socket.getLocalSocketAddress,
//          socketChannel.socket.getSendBufferSize, sendBufferSize,
//          socketChannel.socket.getReceiveBufferSize, recvBufferSize))
      metrics.incrConn()
      processor.accept(socketChannel)
    }
    def openServerSocket(host:String, ports:Array[Int]):Array[ServerSocketChannel]  = {
      ports.map(port => {
        val socketAddress =
          if(host == null || host.trim.isEmpty)
            new InetSocketAddress(port)
          else
            new InetSocketAddress(host, port)
        val serverChannel = ServerSocketChannel.open()
        serverChannel.configureBlocking(false)
        try {
          serverChannel.socket.bind(socketAddress)
          info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostName, port))
        } catch {
          case e: SocketException =>
            throw new RuntimeException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostName, port, e.getMessage), e)
        }
        serverChannel
      })

    }
  }

  private class Processor(val id:Int ,val time: Long,
                          val maxRequestSize: Int, val metrics:Metrics) extends ServerThread {
    private val newConnections = new ConcurrentLinkedQueue[SocketChannel]();
    private val readBuffer = ByteBuffer.allocate(1024);
    private def configureNewConnections() {
      while(newConnections.size() > 0) {
        val channel = newConnections.poll()
//        info("Processor " + id + " listening to new connection from " + channel.socket.getRemoteSocketAddress)
        channel.register(selector, SelectionKey.OP_READ)

      }
    }
    override def run() {
      startupComplete()
      while(isRunning) {
        // setup any new connections that have been queued up
        configureNewConnections()
        // register any new responses for writing
        val ready = selector.select(300)
        if(ready > 0) {
          val keys = selector.selectedKeys()
          val iter = keys.iterator()
          while(iter.hasNext && isRunning) {
            var key: SelectionKey = null
            try {
              key = iter.next
              iter.remove()
              if(key.isReadable)
                read(key)
              else if(key.isWritable)
                write(key)
              else if(!key.isValid){
                info("key is invalid")
                close(key)
              }
              else
                throw new IllegalStateException("Unrecognized key state for processor thread.")
            } catch {
              case e: EOFException => {
                info("Closing socket connection to %s.".format(channelFor(key).socket.getInetAddress))
                close(key)
              }
              case e: Throwable => {
                info("Closing socket for " + channelFor(key).socket.getInetAddress + " because of error" + e)
                close(key)
              }
            }
          }
        }
      }
      info("Closing selector.")
      selector.close()
      shutdownComplete()
    }
    private def channelFor(key: SelectionKey) = key.channel().asInstanceOf[SocketChannel]
    private def close(key: SelectionKey) {
      val channel = channelFor(key)
      info("Closing connection from " + channel.socket.getRemoteSocketAddress())
      channel.socket().close()
      channel.close()
      key.attach(null)
      key.cancel()
    }
    def read(key: SelectionKey) = {
      val socketChannel = channelFor(key)
      readBuffer.clear()
      // Attempt to read off the channel
      var numRead:Int = -1
      try {
        numRead = socketChannel.read(readBuffer);
        if (numRead == -1) {
          // Remote entity shut the socket down cleanly. Do the
          // same from our end and cancel the channel.
          close(key)
        } else{
            metrics.incrReq(numRead)
//          key.attach(ByteBuffer.wrap("pong\r\n".getBytes()))
//          key.interestOps(SelectionKey.OP_WRITE)
        }
      } catch {
        case e: Throwable => {
          info("reading key exception " + e)
          close(key)
        }

      }


    }

    def write(key: SelectionKey) = {
      info("key is in writable")
      val response = key.attachment().asInstanceOf[ByteBuffer]
      if(response == null){
        throw new IllegalStateException("Registered for write interest but no response attached to key.")
      }
      val socket = channelFor(key)
      socket.write(response)
//      info("response bytebuffer stats, hasRemaining %s, position %d, limit %d ".format(response.hasRemaining, response.position()
//        , response.limit()))
      key.interestOps(SelectionKey.OP_READ)

    }
    def accept(socketChannel:SocketChannel) = {
      newConnections.add(socketChannel)
      wakeup()
    }
  }


}
sealed class Metrics(val emitInterval:Int) {
  val startTime =   System.currentTimeMillis
  var lastEmitTime = startTime
  var connections = 0
  var totalRequests = 0
  var bytesReceived = 0
  var requests = 0
  var formattedMsg = "uptime %ds, open connections %d, total requests %d, throughput %dKB/s, req/second %d"
  val timer = new Timer()
  class EmitTimerTask extends TimerTask {
    def run(){

      val totalUptime =  (System.currentTimeMillis - startTime) / 1000
      val uptime = (System.currentTimeMillis - lastEmitTime) / 1000
      val throughput = bytesReceived / 1024 / uptime
      val reqPerSec = requests / uptime
      println(formattedMsg.format(totalUptime, connections, totalRequests, throughput, reqPerSec))
      reset();
    }
  }
  timer.schedule(new EmitTimerTask(),5000, emitInterval)

  def incrConn(){
    connections+=1
  }
  def decrConn(){
    connections-=1
  }
  def incrReq(sizeOfByte:Int){
    bytesReceived+=sizeOfByte
    totalRequests+=1
    requests+=1
  }
  def reset(){
    lastEmitTime = System.currentTimeMillis
    bytesReceived = 0
    requests = 0
  }

}
object serverRunner {
  def main(argv:Array[String]) = {
    val Array(host, ports, numProcessor, sendBufferSize, recvBufferSize, maxRequestSize, metricsEmitInterval) = argv
    val server = new SocketServer(host, ports.split(",").map(_.toInt), numProcessor.toInt
      , sendBufferSize.toInt, recvBufferSize.toInt, maxRequestSize.toInt, metricsEmitInterval.toInt)
    server.startUp()

//    Runtime.getRuntime().addShutdownHook(new Thread() {
//      override def run() = {
//        server.shutDown
//      }
//    })

  }
}
