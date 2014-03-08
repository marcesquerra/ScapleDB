package com.bryghts.scapledb.profecy

import scala.concurrent._
import scala.util.Success
import scala.util.Failure
import scala.concurrent.ExecutionContext

sealed trait CouldHappen[+A]
{

	def headOpt:Option[A]
	def next(implicit ec: ExecutionContext):Prophecy[A]
	def isEmpty:Boolean

}

case object NothingHappened extends AnyRef with CouldHappen[Nothing] {
	override def headOpt = None
	override def next(implicit ec: ExecutionContext):Prophecy[Nothing] = Prophecy(Future.successful(NothingHappened))
	override val isEmpty = true
}

final case class Happened[+A](val headOpt: Some[A], nextProvider: Prophet[A]) extends CouldHappen[A]
{

	override def next(implicit ec: ExecutionContext):Prophecy[A] = {
		val p = Promise[CouldHappen[A]]

		nextProvider.next.onComplete {
			case Success(Some(n)) => p success Happened(n)
			case Success(None)    => p success NothingHappened
			case Failure(t)       => p failure t
		}

		Prophecy(p.future)
	}

	override val isEmpty = false
}

object Happened
{

	def apply[A](content: Prophesied[A]): Happened[A] = Happened(Some(content.element), content.nextProphet)

}
