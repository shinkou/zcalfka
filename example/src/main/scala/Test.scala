// vim: set sw=2 ts=2 tw=0 et:
import java.util.concurrent.atomic.AtomicLong

import org.apache.log4j.{Logger, LogManager}

import com.github.shinkou.zcalfka.consumer.BaseConsumer

class TestConsumer(override val gid: String, override val t: String, override val ep: String) extends BaseConsumer(gid, t, ep) {
  protected var msgcnt = new AtomicLong()

  override def process(buf: java.nio.ByteBuffer) {
    msgcnt.getAndIncrement
  }

  def getMsgcnt() = msgcnt.getAndSet(0L)
}

object Test extends App {
  var consumer = new TestConsumer("test-group", "test-topic", "localhost:2181")
  var logger = LogManager.getLogger(getClass)
  val th = new Thread {
    override def run {
      while(true) {
        Thread.sleep(300000)
        logger.info("Message count: " + consumer.getMsgcnt)
      }
    }
  }
  sys.addShutdownHook(consumer.stop)
  th.start
  consumer.start
  logger.info("Ending...")
}
