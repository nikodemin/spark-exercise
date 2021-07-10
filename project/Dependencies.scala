object Dependencies {

  import sbt._

  sealed trait Module {
    def groupId: String

    def live: List[ModuleID]

    def withVersion(version: String): String => ModuleID = groupId %% _ % version

    def withVersionSimple(version: String): String => ModuleID = groupId % _ % version
  }

  object spark extends Module {
    override def groupId: String = "org.apache.spark"

    override def live: List[sbt.ModuleID] = List("spark-sql").map(withVersion("3.1.2"))
  }

  val live: List[ModuleID] = List(spark).flatMap(_.live)
}
