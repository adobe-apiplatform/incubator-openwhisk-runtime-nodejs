/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package whisk.core.containerpool

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.MessageEntity
import akka.http.scaladsl.unmarshalling.Unmarshal
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.protocol.HttpContext
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try
import spray.json._
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.entity.ActivationResponse.ContainerHttpError
import whisk.core.entity.ActivationResponse._
import whisk.core.entity.ByteSize
import whisk.core.entity.size.SizeLong
import whisk.http.PoolingRestClient

/**
 * This HTTP client is used only in the invoker to communicate with the action container.
 * It allows to POST a JSON object and receive JSON object back; that is the
 * content type and the accept headers are both 'application/json.
 * The reason we still use this class for the action container is a mysterious hang
 * in the Akka http client where a future fails to properly timeout and we have not
 * determined why that is.
 *
 * @param hostname the host name
 * @param timeout the timeout in msecs to wait for a response
 * @param maxResponse the maximum size in bytes the connection will accept
 * @param maxConcurrent the maximum number of concurrent requests allowed (Default is 1)
 */
protected class HttpUtils3(hostname: String,
                           port: Int,
                           timeout: FiniteDuration,
                           maxResponse: ByteSize,
                           maxConcurrent: Int = 1)(implicit logging: Logging, as: ActorSystem)
    extends PoolingRestClient("http", hostname, port, 16 * 1024) {

  def close() = Unit //Try(connection.close())

  /**
   * Posts to hostname/endpoint the given JSON object.
   * Waits up to timeout before aborting on a good connection.
   * If the endpoint is not ready, retry up to timeout.
   * Every retry reduces the available timeout so that this method should not
   * wait longer than the total timeout (within a small slack allowance).
   *
   * @param endpoint the path the api call relative to hostname
   * @param body the JSON value to post (this is usually a JSON objecT)
   * @param retry whether or not to retry on connection failure
   * @return Left(Error Message) or Right(Status Code, Response as UTF-8 String)
   */
  def post(endpoint: String, body: JsValue)(
    implicit tid: TransactionId): Future[Either[ContainerHttpError, ContainerResponse]] = {

    val b = Marshal(body).to[MessageEntity]
    val req = b.map { b =>
      HttpRequest(HttpMethods.POST, endpoint, entity = b)
    }
    request(req).flatMap({ response =>
      if (response.status.isSuccess()) {
        Unmarshal(response.entity.withoutSizeLimit()).to[JsObject].map { o =>
          Right(ContainerResponse(true, o.toString))
        }
      } else {
        // This is important, as it drains the entity stream.
        // Otherwise the connection stays open and the pool dries up.
        Unmarshal(response.entity.withoutSizeLimit()).to[JsObject].map { o =>
          Right(new ContainerResponse(response.status.intValue(), o.toString(), None))
        }

      }
    })
  }
}
object HttpUtils3 {

  /** A helper method to post one single request to a connection. Used for container tests. */
  def post(host: String, port: Int, endPoint: String, content: JsValue, timeout: Duration = 30.seconds)(
    implicit logging: Logging,
    as: ActorSystem,
    ec: ExecutionContext,
    tid: TransactionId): (Int, Option[JsObject]) = {
    val connection = new HttpUtils3(host, port, 90.seconds, 1.MB)
    val response = executeRequest(connection, endPoint, content, Some(HttpClientContext.create()))
    connection.close()
    Await.result(response, timeout)
  }

  /** A helper method to post multiple concurrent requests to a single connection. Used for container tests. */
  def concurrentPost(host: String, port: Int, endPoint: String, contents: Seq[JsValue], timeout: Duration)(
    implicit logging: Logging,
    tid: TransactionId,
    as: ActorSystem,
    ec: ExecutionContext): Seq[(Int, Option[JsObject])] = {
    val connection = new HttpUtils3(host, port, 90.seconds, 1.MB, 20)
    val futureResults = contents.map(content => {
      connection
        .post(endPoint, content)
        .map({
          case Left(e)  => (500, None)
          case Right(r) => (r.statusCode, Try(r.entity.parseJson.asJsObject).toOption)
        })
    })
    val results = Await.result(Future.sequence(futureResults), timeout)
    connection.close()
    results
  }

  private def executeRequest(connection: HttpUtils3, endpoint: String, content: JsValue, context: Option[HttpContext])(
    implicit logging: Logging,
    as: ActorSystem,
    ec: ExecutionContext,
    tid: TransactionId): Future[(Int, Option[JsObject])] = {

    connection.post(endpoint, content) map {
      case Right(r)                   => (r.statusCode, Try(r.entity.parseJson.asJsObject).toOption)
      case Left(NoResponseReceived()) => throw new IllegalStateException("no response from container")
      case Left(Timeout(_))           => throw new java.util.concurrent.TimeoutException()
      case Left(ConnectionError(t: java.net.SocketTimeoutException)) =>
        throw new java.util.concurrent.TimeoutException()
      case Left(ConnectionError(t)) => throw new IllegalStateException(t.getMessage)
    }
  }
}
