/*
 * Copyright (C) 2016  Chun-Kwong Wong
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
