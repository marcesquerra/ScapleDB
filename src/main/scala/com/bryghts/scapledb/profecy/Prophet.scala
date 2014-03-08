package com.bryghts.scapledb.profecy

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Success
import scala.util.Failure
import com.bryghts.scapledb.profecy._





/**
 * Container for the data needed to build a Happening
 *
 * @param A  the type of the recovered element
 */
case class Prophesied[+A](
		    element: A,
		nextProphet: Prophet[A])
{

	/**
	 * Constructs a Happened from this Prophesied
	 */
	def asHappened: Happened[A] = Happened(Some(element), nextProphet)

}



/**
 * A Prophet can retrieve the next content of a prophecy (if any) wrapped in a
 * Future.
 *
 * @param A  the type of elements this Prophet can recover
 */
trait Prophet[+A]
{



	/**
	 * Gets a future that will contain Some(Prophesied) if there are more
	 * elements in the collection.
	 */
	def next(implicit ec: ExecutionContext):Future[Option[Prophesied[A]]]


}

