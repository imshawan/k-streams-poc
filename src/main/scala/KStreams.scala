package org.imshawan.kstreams

import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties
import scala.concurrent.duration._

object KStreams {

  object Domain {
    type UserId = String
    type Profile = String
    type Product = String
    type OrderId = String

    case class Order(orderId: OrderId, user: UserId, products: List[Product], amount: Double)
    case class Discount(profile: Profile, amount: Double) // in percentage points
    case class Payment(orderId: OrderId, status: String)
  }

  object Topics {
    final val OrdersByUserTopic = "orders-by-user"
    final val DiscountProfilesByUserTopic = "discount-profiles-by-user"
    final val DiscountsTopic = "discounts"
    final val OrdersTopic = "orders"
    final val PaymentsTopic = "payments"
    final val PaidOrdersTopic = "paid-orders"
  }

//    List(
//      "orders-by-user",
//      "discount-profiles-by-user",
//      "discounts",
//      "orders",
//      "payments",
//      "paid-orders"
//    ).foreach{ topic =>
//      val cmd = s"kafka-topics --bootstrap-server localhost:9092 --topic $topic --create"
//      println(cmd)
//    }

  import Domain._
  import Topics._

  /*
  Stream processors ->
  1. Source - emits messages
  2. flow - transforms elements along the way (e.g. map)
  3. sink - "ingests" elements
   */

  implicit def serde[A >: Null : Encoder : Decoder]: Serde[A] = {
    val serializer = (a: A) => a.asJson.noSpaces.getBytes()
    val deserializer = (bytes: Array[Byte]) => {
      val string = new String(bytes)
      decode[A](string).toOption
    }

    Serdes.fromFn[A](serializer, deserializer)
  }

  /*
    Topology
   */
  def main(args: Array[String]): Unit = {

    val builder = new StreamsBuilder()

    // KStream -> defines a linear flow kafka streamm of elements from a K Topic
    val userOrdersStream: KStream[UserId, Order] = builder.stream[UserId, Order](OrdersByUserTopic)

    // KTable -> the elements which move through are a bit more static. They are kept inside the topic with a TTL policy
    // They are corresponding to a topic which we call compacted
    val usersProfileTable: KTable[UserId, Profile] = builder.table[UserId, Profile](DiscountProfilesByUserTopic)


    // GlobalKTable - copied to all the nodes whereas KTable is distributed across all the nodes
    val discountProfilesGtable: GlobalKTable[Profile, Discount] = builder.globalTable[Profile, Discount](DiscountsTopic)

    // KStream Transformations: filter, map, mapValues, flatMapValues
    val expensiveOrders = userOrdersStream.filter{
      (userId, order) => order.amount > 1000
    }

    val listOfProducts = userOrdersStream.mapValues {
      (order) => order.orderId
    }

    val productStream = userOrdersStream.flatMapValues(_.products)

    // Join
    val ordersWithUserProfile = userOrdersStream.join(usersProfileTable) {
      (order, profile) => {
        (order, profile)
      }
    }

    val discountedOrderStream = ordersWithUserProfile.join(discountProfilesGtable) (
      { case (userId, (order, profile)) => profile}, // Key of the join - picked from the "left" stream
      { case ((order, profile), discount) => order.copy( amount = order.amount - discount.amount )} // values of the matched records
    )

    // Pick another identifier
    val ordersStream = discountedOrderStream.selectKey((userId, order) => order.orderId)
    val paymentsStream = builder.stream[OrderId, Payment](PaymentsTopic)

    val joinWindow = JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES))
    val joinOrdersPayments = (order: Order, payment: Payment) => if (payment.status == "PAID") Option(order) else Option.empty[Order]
  //  val ordersPaid = ordersStream.join(paymentsStream)(joinOrdersPayments, joinWindow).filter((orderId, maybeOrder) => maybeOrder.nonEmpty)
    val ordersPaid = ordersStream.join(paymentsStream)(joinOrdersPayments, joinWindow).flatMapValues((maybeOrder) => maybeOrder.toIterable)

    // Sink
    ordersPaid.to(PaidOrdersTopic)

    val topology: Topology = builder.build()
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

    val application = new KafkaStreams(topology, props)
    application.start()

    println("Application has started")
  }

}
