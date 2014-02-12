package io.github.dmitrib.elasticsearch.cli

import com.beust.jcommander.{Parameters, Parameter}
import java.io.{InputStreamReader, BufferedReader, FileInputStream}
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.action.deletebyquery.{IndexDeleteByQueryResponse, DeleteByQueryResponse}
import scala.collection.JavaConverters._

@Parameters(commandDescription = "delete a list of documents by id in batches")
object BatchDeleteCommand extends Runnable {
  import EsTool._

  @Parameter(
    names = Array("--batch-size"),
    description = "Number of params to supply in each search request")
  var batchSize = 50000

  @Parameter(
    names = Array("--file"),
    description = "A file with newline-separated 'id TAB [json (not nessesary)]' to , system input will be used if no file is specified")
  var file: String = _

  def run() {
    val stream = Option(file).map(new FileInputStream(_)).getOrElse(System.in)

    val reader = new BufferedReader(new InputStreamReader(stream))
    val it = Iterator.continually(reader.readLine).takeWhile(_ != null).grouped(batchSize)
    for (batch <- it) {
      val req = client.prepareDeleteByQuery(index).setTypes(kind)
      val qb = QueryBuilders.idsQuery(kind)
      qb.addIds()

      val ids = batch flatMap { line =>
        val splitted = line.split("\t")
        if(!splitted(0).isEmpty) Some(splitted(0))
        else None
      }
      qb.addIds(ids : _*)
      req.setQuery(qb)

      val res: DeleteByQueryResponse = req.execute().actionGet()

      res.getHeaders

      res.getIndices.asScala foreach { case (index: String, result: IndexDeleteByQueryResponse) =>
        println(s"$index\tfailedShards:${result.getFailedShards}\tsuccessfulShards:${result.getSuccessfulShards}")
      }
    }

    client.admin().indices().prepareFlush(index).execute().get()

    client.close()
  }
}
