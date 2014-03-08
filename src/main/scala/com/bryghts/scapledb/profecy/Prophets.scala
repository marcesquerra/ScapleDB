package com.bryghts.scapledb.profecy

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Success
import scala.util.Failure
import com.bryghts.scapledb.profecy._



import BucketedProphet._


/**
 * A Prophet that ends a collection of prophecies
 */
case object EmptyProphet extends Prophet[Nothing]
{
	override def next(implicit ec: ExecutionContext) =
		Future.successful(None)
}




/**
 * A Prophet that gets it's content from another prophet to which it performs a
 * map operation
 *
 * @param A  The type of the elements that the source Prophet provides
 * @param U  The type of the elements that this Prophet provides
 *
 * @param in  The prophet that provides the source content
 * @param  f  The function that transforms the elements from the source prophet
 *            into elements of type U
 */
case class MappedProphet[A, +U](in: Prophet[A], f: A => U) extends Prophet[U]
{

	override def next(implicit ec: ExecutionContext):
											Future[Option[Prophesied[U]]] =
		in.next.map
		{
			case Some(s) =>
				Some(Prophesied(
						f(s.element),
						new MappedProphet(s.nextProphet, f)))

			case None =>
				None
		}

}




/**
 * A Prophet that gets it's content from another prophet, but only the ones that
 * satisfy the 'f' predicate
 *
 * @param A  the type of the elements this Prophet provides
 *
 *
 * @param in  The prophet that provides the source content
 * @param  f  The predicate that checks if an specific element from the source
 *            prophet should be provided by this prophet
 */
case class FilteredProphet[A](in: Prophet[A], f: A => Boolean) extends Prophet[A]
{

	override def next(implicit ec: ExecutionContext):
											Future[Option[Prophesied[A]]] =
	{
		in.next.flatMap
		{row =>
			row match
			{
				case Some(s) =>
					if(f(s.element)) Future successful
						Some(Prophesied(
								s.element,
								new FilteredProphet(s.nextProphet, f)))
					else
						new FilteredProphet(s.nextProphet, f).next

				case None =>
					Future successful None
			}
		}
	}
}




/**
 * A prophet that gets its content from a collection of prophecies
 *
 * @param book  The source collection of prophecies
 */
case class BookProphet[+A](book: Prophecy[A]*) extends Prophet[A]
{

	override def next(implicit ec: ExecutionContext):
											Future[Option[Prophesied[A]]] =
	{
		if(book.isEmpty)
			Future successful None
		else
		{
			val p = Promise[Option[Prophesied[A]]]

			book.head.onComplete
			{
				case Success(NothingHappened) =>
					if(book.length == 1)
						p success None
					else
						new BookProphet(book.tail :_*).next.onComplete{
							case Success(s) => p success s
							case Failure(t) => p failure t
						}

				case Success(s) =>
					p success Some(Prophesied(
							s.headOpt.get,
							new BookProphet((s.next +: book.tail) :_*)))

				case Failure(t) => p failure t
			}

			p.future
		}
	}
}




/**
 * BucketedProphet companion object
 */
object BucketedProphet
{



	/**
	 * Contains the content of a bucket of elements of type A, and the token
	 * needed to recover the next bucket, if any.
	 *
	 * @param A  the type of the elements of the bucket
	 * @param T  the type of the token needed to recover a possible continuation
	 *           bucket
	 *
	 * @param   content  the elements contained in this bucket
	 * @param nextToken  the token that identifies the next bucket to recover,
	 *                   or None if there are no more buckets to recover.
	 */
	case class Bucket[+A, T](content: List[A], nextToken: Option[T])




	/**
	 * Defines the signature of a function capable of recovering a bucket of
	 * elements.
	 *
	 * The method receives as an input parameter a "token: Option[T]". It will
	 * be None if the first bucket is being requested or Some(t) with the token
	 * that identifies an specific bucket.
	 *
	 * @param A  the type of the elements the buckets will contain
	 * @param T  the type of the tokens
	 */
	type BucketProvider[A, T] = Option[T] => Future[Bucket[A, T]]




	/**
	 * Builds a new
	 */
	def apply[A, T](nextBucket: BucketProvider[A, T]) =
			new BucketedProphet(nextBucket, None)

}

class BucketedProphet[+A, T] private(
		nextBucket: BucketProvider[A, T],
		nextToken: Option[T]) extends Prophet[A]
{

	override def next(implicit ec: ExecutionContext):Future[Option[Prophesied[A]]] = {
		val p = Promise[Option[Prophesied[A]]]

		val f = nextBucket(nextToken)

		f.onComplete {
			case Success(s) =>

				val cache = s.content

				if(cache.size == 0)
					p success None
				else
				{
					val nt    = s.nextToken
					val head  = cache.head

					if(cache.size == 1)
					{
						if(nt.isEmpty)
							p.success(Some(Prophesied(head, EmptyProphet)))
						else
							p.success(Some(Prophesied(head, new BucketedProphet(nextBucket, nt))))
					}
					else
						p success Some(Prophesied(head, new ContinuationGenericProvider(nextBucket, nt, cache.tail)))
				}

			case Failure(t) =>
				p failure t
		}

		p.future
	}




	private class ContinuationGenericProvider[+A, T](
			nextBucket: BucketProvider[A, T],
			nextToken: Option[T],
			cache: Traversable[A]) extends Prophet[A]
	{
		override def next(implicit ec: ExecutionContext):Future[Option[Prophesied[A]]] = {
			val p = Promise[Option[Prophesied[A]]]

			if(cache.isEmpty)
				p failure(new RuntimeException("Cache should NEVER be empty"))
			else
			{
				if(cache.size == 1)
				{
					if(nextToken.isEmpty)
						p success Some(Prophesied(cache.head, EmptyProphet))
					else
						p success Some (Prophesied(cache.head, new BucketedProphet(nextBucket, nextToken)))
				}
				else
					p success Some(Prophesied(cache.head, new ContinuationGenericProvider(nextBucket, nextToken, cache.tail)))
			}


			p.future
		}

	}
}



class CollectionProphet[+A](in: Traversable[A]) extends Prophet[A]
{


	override def next(implicit ec: ExecutionContext):
											Future[Option[Prophesied[A]]] =
		if(in.isEmpty)
			Future successful None

		else
			Future successful
				Some(Prophesied(in.head, CollectionProphet(in.tail)))

}

object CollectionProphet
{
	def apply[A](in: TraversableOnce[A]) = new CollectionProphet(in.toTraversable)
	def apply[A](in: Traversable[A]) = new CollectionProphet(in)
}



