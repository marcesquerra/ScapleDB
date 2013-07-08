package com.bryghts.scapledb

import com.amazonaws.services.simpledb.AmazonSimpleDB
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.simpledb.AmazonSimpleDBClient
import com.amazonaws.services.simpledb.model.SelectRequest
import scala.concurrent._
import com.amazonaws.services.simpledb.model.PutAttributesRequest
import com.amazonaws.services.simpledb.model.BatchDeleteAttributesRequest
import com.amazonaws.services.simpledb.model.DeleteDomainRequest
import com.amazonaws.services.simpledb.model.CreateDomainRequest
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest
import com.amazonaws.services.simpledb.model.ListDomainsRequest
import com.amazonaws.services.simpledb.model.GetAttributesRequest
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest
import com.amazonaws.services.simpledb.model.DomainMetadataRequest
import java.util.Date
import com.amazonaws.services.simpledb.model.NoSuchDomainException
import com.bryghts.scapledb.providers.StartGenericProvider
import scala.collection.JavaConversions._
import com.amazonaws.services.simpledb.model.{Item => AWSItem}
import com.bryghts.scapledb.results.FutureResult

class SDB private(core: SDBCore)
{
	import SDB._

	/**
	 * Returns information about the domain, including when the domain was
	 * created, the number of items and attributes in the domain, and the size
	 * of the attribute names and values. Or None if the domain name does not
	 * exist.
	 */
	def domainMetadata
				(domainName: String)
				(implicit ec: ExecutionContext):
								Future[Option[DomainMetadata]] =

		// If the domainName is empty or null, I can be sure that domain does
		// not exist
		if(domainName == null || domainName.trim().length() == 0)
			Future.successful(None)

		else
			core
				.domainMetadata(new DomainMetadataRequest(domainName))
				.map {response =>
					Some(DomainMetadata (
							response.getAttributeNameCount(),
							response.getAttributeNamesSizeBytes(),
							response.getAttributeValueCount(),
							response.getAttributeValuesSizeBytes(),
							response.getItemCount(),
							response.getItemNamesSizeBytes(),
							new Date(response
										.getTimestamp()
										.longValue() * 1000l)))
				}
				.recover {case e: NoSuchDomainException => None}


	def listDomains (implicit ec: ExecutionContext) =
		provider
		{nextToken =>

			core
				.listDomains(
					new ListDomainsRequest()
							.withNextToken(nextToken.getOrElse(null)))
				.map{result =>
						(result.getDomainNames().toList, Option(result.getNextToken()))
				}

		}


	def listDomains (paginationSize: Int)(implicit ec: ExecutionContext) =
		provider
		{nextToken =>

			core
				.listDomains(
					new ListDomainsRequest()
							.withMaxNumberOfDomains(paginationSize)
							.withNextToken(nextToken.getOrElse(null)))
				.map{result =>
						(result.getDomainNames().toList, Option(result.getNextToken()))
				}

		}

	def select(query: String, consistenRead: Boolean = false)(implicit ec: ExecutionContext) =
		provider
		{nextToken =>
			core
				.select(
					new SelectRequest(query, consistenRead)
							.withNextToken(nextToken.getOrElse(null)))
				.map{result =>
						(result.getItems().toList.map{item =>
							Item(
								item.getName(),
								item.getAttributes().toList.groupBy(_.getName()).map{row =>
									(row._1, row._2.map{_.getValue()})}
							)
				}, Option(result.getNextToken()))
			}
		}

	def createDomain(name: String)(implicit ec: ExecutionContext) =
		core.createDomain(new CreateDomainRequest(name))

	def getItem(domain: String, item: String, consistentRead: Boolean = false)(implicit ec: ExecutionContext) =
		core
			.getAttributes(new GetAttributesRequest(domain, item).withConsistentRead(consistentRead))
			.map{response =>
					Item(
							item,
							response.getAttributes().toList.groupBy(_.getName()).map{row =>
								(row._1, row._2.map{_.getValue()})
							}
					)
			}

	def getItemAttributes(domain: String, item: String, attributes: Traversable[String], consistentRead: Boolean = false)(implicit ec: ExecutionContext) =
		core
			.getAttributes(new GetAttributesRequest(domain, item).withConsistentRead(consistentRead).withAttributeNames(attributes.toList))
			.map{response =>
					response.getAttributes().toList.groupBy(_.getName()).map{row =>
						(row._1, row._2.map{_.getValue()})
					}
			}

	def getItemAttributes(domain: String, item: String, consistentRead: Boolean)(attributes: String*)(implicit ec: ExecutionContext):Future[Map[String, List[String]]] =
			getItemAttributes(domain, item, attributes.toList, consistentRead)(ec)

	def getItemAttributes(domain: String, item: String)(attributes: String*)(implicit ec: ExecutionContext):Future[Map[String, List[String]]] =
			getItemAttributes(domain, item, attributes.toList, false)(ec)

}

case class DomainMetadata(

	/** The number of unique attribute names in the domain. */
	attributeNameCount: Int,

	/** The total size of all unique attribute names in the domain, in bytes. */
	attributeNamesSizeBytes: Long,

	/** The number of all attribute name/value pairs in the domain. */
	attributeValueCount: Int,

	/** The total size of all attribute values in the domain, in bytes. */
	attributeValuesSizeBytes: Long,

	/** The number of all items in the domain. */
	itemCount: Int,

	/** The total size of all item names in the domain, in bytes. */
	itemNamesSizeBytes: Long,

	/** The data and time when metadata was calculated. In UTC */
	timestamp: Date

)

case class Item(
		name: String,
		attributes: Map[String, List[String]]
)

class SDBCore private(client: AmazonSimpleDB)
{

	def select(request: SelectRequest)(implicit ec: ExecutionContext) =
		future{client.select(request)}

	def putAttributes(request: PutAttributesRequest)(implicit ec: ExecutionContext) =
		future{client.putAttributes(request)}

	def batchDeleteAttributes(request: BatchDeleteAttributesRequest)(implicit ec: ExecutionContext) =
		future{client.batchDeleteAttributes(request)}

	def deleteDomain(request: DeleteDomainRequest)(implicit ec: ExecutionContext) =
		future{client.deleteDomain(request)}

	def createDomain(request: CreateDomainRequest)(implicit ec: ExecutionContext) =
		future{client.createDomain(request)}

	def deleteAttributes(request: DeleteAttributesRequest)(implicit ec: ExecutionContext) =
		future{client.deleteAttributes(request)}

	def listDomains(request: ListDomainsRequest)(implicit ec: ExecutionContext) =
		future{client.listDomains(request)}

	def getAttributes(request: GetAttributesRequest)(implicit ec: ExecutionContext) =
		future{client.getAttributes(request)}

	def batchPutAttributes(request: BatchPutAttributesRequest)(implicit ec: ExecutionContext) =
		future{client.batchPutAttributes(request)}

	def domainMetadata(request: DomainMetadataRequest)(implicit ec: ExecutionContext) =
		future{client.domainMetadata(request)}

	def listDomains()(implicit ec: ExecutionContext) =
		future{client.listDomains()}

}



object SDBCore
{

	def apply() =
		new SDBCore(new AmazonSimpleDBClient())

	def apply(awsCredentials: AWSCredentials) =
		new SDBCore(new AmazonSimpleDBClient(awsCredentials))

	def apply(awsCredentials: AWSCredentials, clientConfiguration: ClientConfiguration) =
		new SDBCore(new AmazonSimpleDBClient(awsCredentials, clientConfiguration))

	def apply(awsCredentialsProvider: AWSCredentialsProvider) =
		new SDBCore(new AmazonSimpleDBClient(awsCredentialsProvider))

	def apply(awsCredentialsProvider: AWSCredentialsProvider, clientConfiguration: ClientConfiguration) =
		new SDBCore(new AmazonSimpleDBClient(awsCredentialsProvider, clientConfiguration))

	def apply(clientConfiguration: ClientConfiguration) =
		new SDBCore(new AmazonSimpleDBClient(clientConfiguration))

}




object SDB
{

	def apply() =
		new SDB(SDBCore())

	def apply(awsCredentials: AWSCredentials) =
		new SDB(SDBCore(awsCredentials))

	def apply(awsCredentials: AWSCredentials, clientConfiguration: ClientConfiguration) =
		new SDB(SDBCore(awsCredentials, clientConfiguration))

	def apply(awsCredentialsProvider: AWSCredentialsProvider) =
		new SDB(SDBCore(awsCredentialsProvider))

	def apply(awsCredentialsProvider: AWSCredentialsProvider, clientConfiguration: ClientConfiguration) =
		new SDB(SDBCore(awsCredentialsProvider, clientConfiguration))

	def apply(clientConfiguration: ClientConfiguration) =
		new SDB(SDBCore(clientConfiguration))

	private def provider[A](f: Option[String] => Future[(List[A], Option[String])])(implicit ec: ExecutionContext) =
		FutureResult(new StartGenericProvider(f))
}
