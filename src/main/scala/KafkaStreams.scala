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

object KafkaStreams {

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

  def main(args: Array[String]): Unit = {
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
  }

  /*
  Stream processors ->
  1. Source - emits messages
  2. flow - transforms elements along the way (e.g. map)
  3. sink - "ingests" elements
   */
}
