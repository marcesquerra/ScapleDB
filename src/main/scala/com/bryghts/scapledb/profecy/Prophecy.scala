package com.bryghts.scapledb.profecy

import scala.concurrent._
import scala.util.Success
import scala.util.Failure
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.Builder
import scala.annotation.tailrec

case class Prophecy[+A](in:Future[CouldHappen[A]])(implicit ec: ExecutionContext) extends Awaitable[CouldHappen[A]]
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

	def map[U](f: A => U): Prophecy[U] =
		Prophecy(MappedProphet(BookProphet(this), f))

	def filter(f: A => Boolean): Prophecy[A] =
		Prophecy(FilteredProphet(BookProphet(this), f))

	def withFilter(f: A => Boolean): Prophecy[A] = filter(f)

	def ready(atMost: Duration)(implicit permit: CanAwait) = {
		Await.ready(toList, atMost); this
	}

	def result(atMost: Duration)(implicit permit: CanAwait) = {
		Await.ready(toList, atMost)
		in.value.get.get
	}


	def ++[R >: A, B <: R](that: Prophecy[B]): Prophecy[R] = Prophecy[R](BookProphet[R](this, that))
	def ++:[R >: A, B <: R](that: Prophecy[B]): Prophecy[R] = Prophecy[R](BookProphet[R](that, this))
	def ++[R >: A, B <: R](that: TraversableOnce[B]): Prophecy[R] =
		Prophecy[R](BookProphet[R](this, Prophecy(CollectionProphet[R](that))))

	def ++:[R >: A, B <: R](that: TraversableOnce[B]): Prophecy[R] = Prophecy[R](BookProphet[R](Prophecy(CollectionProphet[R](that)), this))

	def foldLeft[B](z: B)(op: (B, A) => B): Future[B] = {

		val p = Promise[B]

		onComplete{
			case Success(NothingHappened) => p success z
			case Success(Happened(Some(h), n))  =>
				Prophecy(n).foldLeft(op(z, h))(op).onComplete{
					case Success(s) => p success s
					case Failure(t) => p failure t
				}
			case Failure(t)               => p failure t
		}

		p.future
	}

	def foldRight[B](z: B)(op: (A, B) => B): Future[B] = {

		val p = Promise[B]

		onComplete{
			case Success(NothingHappened) => p success z
			case Success(Happened(Some(h), n))  =>
				Prophecy(n).foldRight(z)(op).onComplete{
					case Success(s) => p success op(h, s)
					case Failure(t) => p failure t
				}
			case Failure(t) => p failure t
		}

		p.future
	}

	def /:[B](z: B)(op: (B, A) => B): Future[B] = foldLeft(z)(op)

	def :\[B](z: B)(op: (A, B) => B): Future[B] = foldRight(z)(op)

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

	def collect[B](pf: PartialFunction[A, B]): Prophecy[B] = filter(pf.isDefinedAt(_)).map(pf(_))
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

	def size: Future[Int] = {
		val p = Promise[Int]

		onComplete {
			case Success(NothingHappened) => p success 0
			case Success(Happened(h, t))  =>
				Prophecy(t).size.onComplete{
					case Success(s) => p success s + 1
					case Failure(t) => p failure t
				}
			case Failure(t)               => p failure t
		}

		p.future
	}

	def partition(p: A => Boolean): (Prophecy[A], Prophecy[A]) = (filter(p), filter(a => !p(a)))

	def forall(f: A => Boolean): Future[Boolean] = {
		val p = Promise[Boolean]

		onComplete {
			case Success(NothingHappened) => p success true
			case Success(Happened(Some(h), t))  =>
				if(!f(h))
					p success false
				else
					Prophecy(t).forall(f).onComplete{
						case Success(s) => p success s
						case Failure(t) => p failure t
					}
			case Failure(t)               => p failure t
		}

		p.future
	}

	def exists(f: A => Boolean): Future[Boolean] = forall(f).map{! _}

	def count(f: A => Boolean): Future[Int] = {
		val p = Promise[Int]

		onComplete {
			case Success(NothingHappened) => p success 0
			case Success(Happened(Some(h), t))  =>
				val d = if(f(h)) 1 else 0

				Prophecy(t).count(f).onComplete{
					case Success(s) => p success s + d
					case Failure(t) => p failure t
				}
			case Failure(t)               => p failure t
		}

		p.future
	}

	def find(f: A => Boolean): Future[Option[A]] =
	{
		val p = Promise[Option[A]]

		onComplete {
			case Success(NothingHappened) => p success None
			case Success(Happened(Some(h), t))  =>
				if(f(h))
					p success Some(h)
				else
					Prophecy(t).find(f).onComplete{
						case Success(s) => p success s
						case Failure(t) => p failure t
					}
			case Failure(t)               => p failure t
		}

		p.future
	}

  /* The following methods are inherited from TraversableLike
   *
  override def reduceLeft[B >: A](op: (B, A) => B): B
  override def reduceLeftOption[B >: A](op: (B, A) => B): Option[B]
  override def reduceRight[B >: A](op: (A, B) => B): B
  override def reduceRightOption[B >: A](op: (A, B) => B): Option[B]
  override def head: A
  override def headOption: Option[A]
  override def tail: Traversable[A]
  override def last: A
  override def lastOption: Option[A]
  override def init: Traversable[A]
  override def take(n: Int): Traversable[A]
  override def drop(n: Int): Traversable[A]
  override def slice(from: Int, until: Int): Traversable[A]
  override def takeWhile(p: A => Boolean): Traversable[A]
  override def dropWhile(p: A => Boolean): Traversable[A]
  override def span(p: A => Boolean): (Traversable[A], Traversable[A])
  override def splitAt(n: Int): (Traversable[A], Traversable[A])
  override def copyToBuffer[B >: A](dest: Buffer[B])
  override def copyToArray[B >: A](xs: Array[B], start: Int, len: Int)
  override def copyToArray[B >: A](xs: Array[B], start: Int)
  override def toArray[B >: A : ClassTag]: Array[B]
  override def toList: List[A]
  override def toIterable: Iterable[A]
  override def toSeq: Seq[A]
  override def toStream: Stream[A]
  override def sortWith(lt : (A,A) => Boolean): Traversable[A]
  override def mkString(start: String, sep: String, end: String): String
  override def mkString(sep: String): String
  override def mkString: String
  override def toString
  override def stringPrefix : String
  override def view
  override def view(from: Int, until: Int): TraversableView[A, Traversable[A]]

  override def flatMap[B, That](f: A => GenTraversableOnce[B])(implicit bf: CanBuildFrom[Traversable[A], B, That]): That
  override def groupBy[K](f: A => K): Map[K, Traversable[A]]
  override def remove(p: A => Boolean): Traversable[A]
  */
}


object Prophecy
{
	def apply[A](provider: Prophet[A])(implicit ec: ExecutionContext): Prophecy[A] = {
		val p = Promise[CouldHappen[A]]

		provider.next.onComplete{
			case Success(None) =>
				p success NothingHappened
			case Success(Some(s)) =>
				p success Happened(s)
			case Failure(t) =>
				p failure t
		}

		Prophecy(p.future)
	}
}


