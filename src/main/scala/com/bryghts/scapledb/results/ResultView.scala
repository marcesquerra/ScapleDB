package com.bryghts.scapledb.results

import scala.concurrent._
import scala.util.Success
import scala.util.Failure
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.Builder

case object EmptyFutureProvider extends FutureProvider[Nothing] {
	override def next(implicit ec: ExecutionContext) = Future.successful(None)
}

sealed trait ResultView[+A]
{
	def headOpt:Option[A]
	def next(implicit ec: ExecutionContext):FutureResult[A]
	def isEmpty:Boolean
}

case object EmptyResultView extends AnyRef with ResultView[Nothing]
{
	override def headOpt = None
	override def next(implicit ec: ExecutionContext):FutureResult[Nothing] = FutureResult(Future.successful(EmptyResultView))
	override def isEmpty:Boolean = true
}

final case class Result[+A](val headOpt: Option[A], nextProvider: FutureProvider[A]) extends ResultView[A]
{

	override def next(implicit ec: ExecutionContext):FutureResult[A] = {
		val p = Promise[ResultView[A]]

		nextProvider.next.onComplete {
			case Success(Some(n)) =>
				p success Result(n)
			case Success(None)    => p success EmptyResultView
			case Failure(t)       => p failure t
		}

		FutureResult(p.future)
	}

	def isEmpty:Boolean = false
}

object Result
{
	def apply[A](content: NextContent[A]): Result[A] = Result(Some(content.element), content.nextContentProvider)
}

case class FutureResult[+A](in:Future[ResultView[A]])(implicit ec: ExecutionContext) extends Awaitable[ResultView[A]]
{
	val onComplete = in.onComplete _

	def isEmpty:Future[Boolean] = in.map(_.isEmpty)

	def toList:Future[List[A]] = {
		val p = Promise[List[A]]

		onComplete {
			case Success(s) =>
				if(s.isEmpty) p success Nil
				else
					s.next.toList.onComplete{
						case Success(l) =>
							p success s.headOpt.get :: l
						case Failure(t) => p failure t
					}
			case Failure(t) => p failure t
		}

		p.future
	}

	def foreach[U](f: A => U): Unit = {
		in.onComplete{
			case Success(a) =>
				if(!a.isEmpty)
				{
					f(a.headOpt.get)
					a.next.foreach(f)
				}
			case _ =>
		}
	}

	def map[U](f: A => U): FutureResult[U] = {
		val p = Promise[ResultView[U]]

		in.onComplete{
			case Success(EmptyResultView) =>
				p success EmptyResultView
			case Success(Result(headOpt, nextProvider)) =>
				p success Result(headOpt map f, new MappedFutureProvider(nextProvider, f))
			case Failure(t) =>
				p failure t
		}

		FutureResult(p.future)
	}

	def filter(f: A => Boolean): FutureResult[A] = {
		val p = Promise[ResultView[A]]

		in.onComplete{
			case Success(EmptyResultView) =>
				p success EmptyResultView

			case Success(Result(headOpt, nextProvider)) =>
				val continuationProvider = new FilteredFutureProvider(nextProvider, f)

				if(f(headOpt.get))
					p success Result(headOpt, continuationProvider)
				else
					continuationProvider.next.onComplete
					{
						case Success(Some(s)) =>
							p success Result(s)
						case Success(None) =>
							p success EmptyResultView
						case Failure(t) => p failure t
					}

			case Failure(t) =>
				p failure t
		}

		FutureResult(p.future)
	}

	def withFilter(f: A => Boolean): FutureResult[A] = filter(f)

	def ready(atMost: Duration)(implicit permit: CanAwait) = {
		Await.ready(toList, atMost); this
	}

	def result(atMost: Duration)(implicit permit: CanAwait) = {
		Await.ready(toList, atMost)
		in.value.get.get
	}

	// TODO
	//def ++[B >: A, That](that: ResultView[B])(implicit bf: CanBuildFrom[ResultView[A], B, That]): That = ???

	private def addString(sb: StringBuilder, isFirst: Boolean, before: String, sep: String, after: String): Future[StringBuilder] = {
		val p = Promise[StringBuilder]

		if(isFirst)
			sb.append(before)

		onComplete {
			case Success(s) =>
				if(s.isEmpty) {
					if(isFirst)
						sb.append(after)
					p success sb
				}
				else
					sb.append(s.headOpt.get)
					if(!isFirst)
						sb.append(sep)

					s.next.addString(sb, false, before, sep, after).onComplete{
						case Success(l) =>
							if(isFirst)
								sb.append(after)
							p success sb
						case Failure(t) => p failure t
					}
			case Failure(t) => p failure t
		}

		p.future

	}

	def addString(sb: StringBuilder, start: String, sep: String, end: String): Future[StringBuilder] = addString(sb, true, start, sep, end)
	def addString(sb: StringBuilder, sep: String): Future[StringBuilder] = addString(sb, "", sep, "")
	def addString(sb: StringBuilder): Future[StringBuilder] = addString(sb, "", "", "")

	def collect[B](pf: PartialFunction[A, B]): FutureResult[B] = filter(pf.isDefinedAt(_)).map(pf(_))
	def collectFirst[B](pf: PartialFunction[A, B]): Future[Option[B]] = {
		val p = Promise[Option[B]]

		collect(pf).onComplete{
			case Success(s) =>
				if(s.isEmpty) p success None
				else p success Some(s.headOpt.get)
			case Failure(t) => p failure t
		}

		p.future
	}
}


object FutureResult
{
	def apply[A](provider: FutureProvider[A])(implicit ec: ExecutionContext): FutureResult[A] = {
		val p = Promise[ResultView[A]]

		provider.next.onComplete{
			case Success(None) =>
				p success EmptyResultView
			case Success(Some(s)) =>
				p success Result(s)
			case Failure(t) =>
				p failure t
		}

		FutureResult(p.future)
	}
}


