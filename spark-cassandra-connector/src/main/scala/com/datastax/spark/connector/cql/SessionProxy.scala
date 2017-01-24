package com.datastax.spark.connector.cql

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}

import com.datastax.spark.connector.util.{ConfigParameter, Logging}
import com.datastax.driver.core.{RegularStatement, Session, SimpleStatement}
import org.apache.spark.SparkEnv

import scala.util.Try

/** Wraps a `Session` and intercepts:
  *  - `close` method to invoke `afterClose` handler
  *  - `prepare` methods to cache `PreparedStatement` objects. */
class SessionProxy(session: Session, afterClose: Session => Any) extends InvocationHandler {

  private var closed = false

  override def invoke(proxy: Any, method: Method, args: Array[AnyRef]) = {
    try {
      val StringClass = classOf[String]
      val RegularStatementClass = classOf[String]

      (method.getName, method.getParameterTypes) match {
        case ("close", Array()) =>
          null
        case ("closeUnderlying", Array()) =>
          session.close()
          null
        case ("isClosed", Array()) =>
          closed.asInstanceOf[AnyRef]
        case ("prepare", Array(StringClass)) =>
          PreparedStatementCache.prepareStatement(session, new SimpleStatement(args(0).asInstanceOf[String]))
        case ("prepare", Array(RegularStatementClass)) =>
          PreparedStatementCache.prepareStatement(session, args(0).asInstanceOf[RegularStatement])
        case _ =>
          try {
            method.invoke(session, args: _*)
          }
          catch {
            case e: InvocationTargetException =>
              throw e.getCause
          }
      }
    }
    finally {
      if (method.getName == "close" && !closed) {
        closed = true
        afterClose(session)
      }
    }
  }
}

object SessionProxy extends Logging {


  val SessionInterfaceParam = new ConfigParameter[String](
    name = "spark.cassandra.connection.session_interface",
    section = CassandraConnectorConf.ReferenceSection,
    default = "com.datastax.driver.core.Session",
    description =
      """(Expert Use Only) The interface that the Session Proxy should use when connecting
        |with Cassandra.
      """.stripMargin
  )

  val Properties = Set(SessionInterfaceParam)

  lazy val sessionInterface = Class.forName(
    Try(SparkEnv.get.conf
      .get(SessionInterfaceParam.name, SessionInterfaceParam.default))
      .recover { case ex =>
        logWarning(
          s"""Defaulting to ${SessionInterfaceParam.name} to
             |${SessionInterfaceParam.default}: $ex""".stripMargin)
        SessionInterfaceParam.default
      }.get
  )

  /** Creates a new `SessionProxy` delegating to the given `Session`.
    * The proxy adds prepared statement caching functionality. */
  def wrap(session: Session): Session =
    wrapWithCloseAction(session)(_ => ())

  /** Creates a new `SessionProxy` delegating to the given `Session`.
    * Additionally registers a callback on `Session#close` method.
    * @param afterClose code to be invoked after the session has been closed */
  def wrapWithCloseAction(session: Session)(afterClose: Session => Any): Session =
    Proxy.newProxyInstance(
      session.getClass.getClassLoader,
      Array(sessionInterface),
      new SessionProxy(session, afterClose)).asInstanceOf[Session]
}