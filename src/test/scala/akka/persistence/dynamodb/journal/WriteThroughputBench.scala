/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import akka.actor._
import akka.persistence._
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.scaladsl.GraphDSL.Implicits._
import java.util.concurrent.ThreadLocalRandom
import org.HdrHistogram.Histogram
import com.typesafe.config.ConfigFactory
import java.util.UUID
import scala.concurrent.duration._

object WriteThroughputBench extends App with DynamoDBUtils {

  def rnd              = ThreadLocalRandom.current()
  final val oneBillion = 1000L * 1000 * 1000

  class H private (private val entries: Map[Int, Int]) {
    def this(i: Int) = this(Map(i -> 1))
    def this() = this(Map.empty[Int, Int])
    def +(other: H): H = {
      val merged =
        other.entries.foldLeft(entries)((acc, pair) => acc.updated(pair._1, pair._2 + entries.getOrElse(pair._1, 0)))
      new H(merged)
    }
    def record(value: Int): H = new H(entries.updated(value, entries.getOrElse(value, 0) + 1))
    override def toString: String =
      if (entries.nonEmpty) {
        val max = entries.keys.max
        (0 to max).map(entries.getOrElse(_, 0)).mkString("[", ",", "]")
      } else "empty"
  }

  case object Go
  case class Report(endToEnd: Histogram, calls: Histogram, retries: H) {
    def +(other: Report): Report = {
      endToEnd.add(other.endToEnd)
      calls.add(other.calls)
      Report(endToEnd, calls, retries + other.retries)
    }
  }
  object Report {
    def apply(): Report                   = Report(new Histogram(3), new Histogram(3), new H)
    def endToEnd(h: Histogram): Report    = Report(h, new Histogram(3), new H)
    def calls(h: Histogram, r: H): Report = Report(new Histogram(3), h, r)
  }

  class Writer(stats: ActorRef) extends PersistentActor {
    val event = new Array[Byte](100)
    rnd.nextBytes(event)

    var nextReport    = System.nanoTime + oneBillion
    val histo         = new Histogram(3)
    val persistenceId = UUID.randomUUID().toString

    self ! Go

    def receiveCommand = {
      case Go =>
        val start = System.nanoTime
        persist(event) { _ =>
          val end = System.nanoTime
          histo.recordValue(end - start)
          if (end > nextReport) {
            stats ! Report.endToEnd(histo.copy())
            histo.reset()
            nextReport += oneBillion
          }
          self ! Go
        }
    }
    def receiveRecover = Actor.emptyBehavior
  }

  val config =
    ConfigFactory
      .systemProperties()
      .withFallback(
        ConfigFactory
          .parseString("""
my-dynamodb-journal {
  journal-table = "WriteThroughputBench"
  endpoint = ${?AWS_DYNAMODB_ENDPOINT}
  aws-access-key-id = ${?AWS_ACCESS_KEY_ID}
  aws-secret-access-key = ${?AWS_SECRET_ACCESS_KEY}
  aws-client-config {
    max-connections = 100
  }
  plugin-dispatcher = "dispatcher"
  replay-dispatcher = "dispatcher"
  client-dispatcher = "dispatcher"
}
dispatcher {
  type = Dispatcher
  executor = "fork-join-executor"
  fork-join-executor {
    parallelism-min = 4
    parallelism-max = 8
  }
}
akka.actor.default-dispatcher.fork-join-executor.parallelism-max = 4
writers = 1000
writer-dispatcher {
  type = Dispatcher
  executor = "fork-join-executor"
  fork-join-executor {
    parallelism-min = 2
    parallelism-max = 2
  }
}
""").resolve)
      .withFallback(ConfigFactory.load())

  implicit val system       = ActorSystem("WriteThroughputBench", config)
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withInputBuffer(1, 1))

  /*
   * You will want to make sure that the table is deployed with the proper values for
   * read and write throughput; the default is 10/10 which is incredibly low, but defaulting
   * to larger values can burn through a big budget very quickly.
   */
  ensureJournalTableExists()

  val writers = system.settings.config.getInt("writers")

  val endToEnd = Source
    .actorRef[Report](3 * writers, OverflowStrategy.dropHead)
    .conflate(_ + _)
    .prepend(Source.single(Report()))
    .expand(Iterator.continually(_))
    .withAttributes(Attributes.asyncBoundary)

  val calls = Source
    .actorRef[LatencyReport](1000, OverflowStrategy.dropNew)
    .conflateWithSeed(r => ({ val h = new Histogram(3); h.recordValue(r.nanos); h }, new H(r.retries))) {
      case ((hist, h), LatencyReport(nanos, retries)) =>
        hist.recordValue(nanos)
        (hist, h.record(retries))
    }
    .map(p => Report.calls(p._1, p._2))
    .prepend(Source.single(Report()))
    .expand(Iterator.continually(_))
    .withAttributes(Attributes.asyncBoundary)

  val (eRef, cRef) =
    RunnableGraph
      .fromGraph(GraphDSL.create(endToEnd, calls)(Keep.both) { implicit b => (e, c) =>
        val zip = b.add(ZipWith((_: Unit, er: Report, cr: Report) => er + cr))
        Source.tick(1.second, 1.second, ()) ~> zip.in0
        e ~> zip.in1
        c ~> zip.in2
        zip.out ~> Sink.foreach(printStats)
        ClosedShape
      })
      .run()

  println(s"starting $writers writers")
  val props      = Props(new Writer(eRef)).withDispatcher("writer-dispatcher")
  val writerRefs = (1 to writers).map(_ => system.actorOf(props))

  Persistence(system).journalFor("") ! SetDBHelperReporter(cRef)

  println("press <enter> to stop")
  scala.io.StdIn.readLine()

  system.terminate()
  client.shutdown()

  def printStats(r: Report): Unit = {
    def p(h: Histogram, pc: Double) = h.getValueAtPercentile(pc) / 1000000d
    def perc(h: Histogram) =
      f"50=${p(h, 0.5)}%5.1f 90=${p(h, 0.9)}%5.1f 99=${p(h, 0.99)}%5.1f 99.9=${p(h, 0.999)}%5.1f 99.99=${p(h, 0.9999)}%5.1f"
    println(f"count ${r.endToEnd.getTotalCount}%6d/s  percentiles: endToEnd(${perc(r.endToEnd)}) calls(${perc(
      r.calls)}) retries: ${r.retries}")
  }
}
