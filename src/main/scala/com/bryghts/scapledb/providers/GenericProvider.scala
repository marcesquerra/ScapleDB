package com.bryghts.scapledb.providers

import com.bryghts.scapledb.SDBCore
import com.bryghts.scapledb.results.FutureProvider
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.ExecutionContext
import com.amazonaws.services.simpledb.model.ListDomainsRequest
import scala.util.Success
import scala.util.Failure
import scala.collection.JavaConversions._
import com.bryghts.scapledb.results.EmptyFutureProvider
import com.bryghts.scapledb.results.NextContent

class StartGenericProvider[+A, T](
		nextBucket: Option[T] => Future[(List[A], Option[T])],
		nextToken: Option[T]) extends FutureProvider[A]
{

	def this(
		nextBucket: Option[T] => Future[(List[A], Option[T])]) = this(nextBucket, None)

	override def next(implicit ec: ExecutionContext):Future[Option[NextContent[A]]] = {
		val p = Promise[Option[NextContent[A]]]

		val f = nextBucket(nextToken)

		f.onComplete {
			case Success(s) =>

				val cache = s._1

				if(cache.size == 0)
					p success None
				else
				{
					val nt    = s._2
					val head  = cache.head

					if(cache.size == 1)
					{
						if(nt.isEmpty)
							p.success(Some(NextContent(head, EmptyFutureProvider)))
						else
							p.success(Some(NextContent(head, new StartGenericProvider(nextBucket, nt))))
					}
					else
						p success Some(NextContent(head, new ContinuationGenericProvider(nextBucket, nt, cache.tail)))
				}

			case Failure(t) =>
				p failure t
		}

		p.future
	}

}


class ContinuationGenericProvider[+A, T](
		nextBucket: Option[T] => Future[(List[A], Option[T])],
		nextToken: Option[T],
		cache: List[A]) extends FutureProvider[A]
{
	override def next(implicit ec: ExecutionContext):Future[Option[NextContent[A]]] = {
		val p = Promise[Option[NextContent[A]]]

		if(cache.isEmpty)
			p failure(new RuntimeException("Cache should NEVER be empty"))
		else
		{
			if(cache.size == 1)
			{
				if(nextToken.isEmpty)
					p success Some(NextContent(cache.head, EmptyFutureProvider))
				else
					p success Some (NextContent(cache.head, new StartGenericProvider(nextBucket, nextToken)))
			}
			else
				p success Some(NextContent(cache.head, new ContinuationGenericProvider(nextBucket, nextToken, cache.tail)))
		}


		p.future
	}

}
