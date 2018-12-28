//import breeze.linalg.max
//import breeze.linalg.max
//import breeze.linalg.rank
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

object Windows{
  case class Salary(depName:String, empNo: Long, salary: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL example")
      .master("local")
      .getOrCreate()

    import spark.implicits._

     val empSalary = Seq(Salary("sales",1,1000),
       Salary("personnel", 2, 3900),
       Salary("sales", 3, 4800),
       Salary("sales", 4, 4800),
       Salary("personnel", 5, 3500),
       Salary("develop", 7, 4200),
       Salary("develop", 8, 6000),
       Salary("develop", 9, 4500),
       Salary("develop", 10, 5200),
       Salary("develop", 11,5200)).toDS()

    empSalary.printSchema()
    val byDepName = Window.partitionBy("depName").orderBy("salary")
    val result = empSalary.withColumn("avg",rank().over (byDepName))
    result.show()

    empSalary.agg(avg(empSalary("salary")), max(empSalary("salary"))).show()
    empSalary.withColumn("salary1",lag("salary",1,1000).over(byDepName)).show()
    empSalary.withColumn("salary1",lead("salary",1,1000).over(byDepName)).show()


  }
}
