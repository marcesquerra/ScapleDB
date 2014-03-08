package com.bryghts.scapledb

import com.amazonaws.auth.BasicAWSCredentials
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success
import scala.util.Failure
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import com.amazonaws.services.simpledb.AmazonSimpleDBClient
import scala.concurrent.Awaitable

object Sample extends App
{
	def await[A](block: => Awaitable[A]) =
		Await.result(block, Duration.Inf)

	val credentials = new BasicAWSCredentials("AKIAIVL4YR7HYLIM2Y2Q", "h9Sd5YGZsL7jTvtyHru9CRf4Iw7BsmGD46Yo5wAk")

	val core = SDBCore(credentials)
	val client = SDB(credentials)

//	val f = core.listDomains
//
////	f.onComplete {
////		case Success(s) =>
////			println("Here")
////			for(i <- 0 until s.getDomainNames().size())
////				println(s.getDomainNames().get(i))
////
////		case Failure(t) =>
////			t.printStackTrace()
////	}
//
//	val r = Await.result(f, Duration.Inf)
//
////	val client = new AmazonSimpleDBClient(credentials)
////
////	val r = client.listDomains().getDomainNames()
////
//	for(i <- 0 until r.getDomainNames().size())
//			println(r.getDomainNames().get(i))
//
//	val m = Await.result(client.domainMetadata("beers_db"), Duration.Inf)
//
//	println(m.getOrElse("Domain Not Found"))

	val l = client.listDomains.map{_.toUpperCase()}.filter{!_.endsWith("A")}

	l foreach println

	await {l}

	val l2 = client.select("SELECT * FROM beers_db", true)

	for(i <- l2 if i.name.startsWith("N")) println(i)
//	{i =>
//		println(i.name + ": {")
//		i.attributes.foreach{a =>
//			println(a._1 + ": " + a._2.mkString("[", ", ", "]"))
//		}
//		println("}")
//	}

	await {l2}
	for(i <- 0 to (Int.MaxValue - 1))();
	for(i <- 0 to (Int.MaxValue - 1))();
}

