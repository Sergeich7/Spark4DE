import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import scala.io.Source._
import java.io._

object lab01 {

  def main(args: Array[String]): Unit = {

    val film_id = "98"

    val hist_film: Array[Int] = Array(0, 0, 0, 0, 0)
    val hist_all: Array[Int] = Array(0, 0, 0, 0, 0)

    fromFile("u.data").getLines().foreach { line =>
      val Array(user, film, rating, time) = line.split("\t").map(_.trim)
      val r = rating.toInt
      hist_all(r - 1) += 1
      if (film == film_id) hist_film(r - 1) += 1
    }

    val json = ("hist_film" -> hist_film.toList) ~ ("hist_all" -> hist_all.toList)
    val result = compact(render(json))

    val fileWriter = new FileWriter(new File("lab01.json"))
    fileWriter.write(result)
    fileWriter.close()

    println(result)
  }
}