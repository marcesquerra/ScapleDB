package com.bryghts.scapledb.results

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

case class NextContent[+A](element: A, nextContentProvider: FutureProvider[A])


trait FutureProvider[+A]{

	def next(implicit ec: ExecutionContext):Future[Option[NextContent[A]]]

}


class MappedFutureProvider[A, +U](provider: FutureProvider[A], f: A => U) extends FutureProvider[U]{

	override def next(implicit ec: ExecutionContext):Future[Option[NextContent[U]]] =
		provider.next.map
		{
			case Some(s) => Some(NextContent(f(s.element), new MappedFutureProvider(s.nextContentProvider, f)))
			case None => None
		}

}

class FilteredFutureProvider[+A](provider: FutureProvider[A], f: A => Boolean) extends FutureProvider[A]{

	override def next(implicit ec: ExecutionContext):Future[Option[NextContent[A]]] =
	{
		provider.next.flatMap
		{row =>
			row match
			{
				case Some(s) =>
					if(f(s.element)) Future successful  Some(NextContent(s.element, new FilteredFutureProvider(s.nextContentProvider, f)))
					else
						new FilteredFutureProvider(s.nextContentProvider, f).next
				case None => Future successful None
			}
		}
	}
}
