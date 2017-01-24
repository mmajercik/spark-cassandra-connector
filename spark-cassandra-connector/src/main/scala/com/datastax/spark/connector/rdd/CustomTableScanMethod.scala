package com.datastax.spark.connector.rdd

import com.datastax.driver.core.{Row, Session, Statement}
import com.datastax.spark.connector.CassandraRowMetadata
import com.datastax.spark.connector.rdd.reader.PrefetchingResultSetIterator

trait CustomTableScanMethod {

  def getScanMethod(
    readConf: ReadConf,
    session: Session,
    columnNames: IndexedSeq[String]): Statement => (Iterator[Row], CassandraRowMetadata)

}

object DefaultTableScanMethod extends CustomTableScanMethod {
  override def getScanMethod(
    readConf: ReadConf,
    session: Session,
    columnNames: IndexedSeq[String]): (Statement) => (Iterator[Row], CassandraRowMetadata) = {
      case statement: Statement =>
        val rs = session.execute(statement)
        val columnMetaData = CassandraRowMetadata.fromResultSet(columnNames, rs)
        val iterator = new PrefetchingResultSetIterator(rs, readConf.fetchSizeInRows)
        (iterator, columnMetaData)
  }
}


object DummyTableScanMethod extends CustomTableScanMethod {

  val nie = new NotImplementedError("This is just a test method don't use it.")


  override def getScanMethod(
    readConf: ReadConf,
    session: Session,
    columnNames: IndexedSeq[String]): (Statement) => (Iterator[Row], CassandraRowMetadata) = {

    throw nie
  }
}
