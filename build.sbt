// *****************************************************************************
// Projects
// *****************************************************************************

// Calculate the current year for usage in copyright notices and license headers.
lazy val currentYear: Int = java.time.OffsetDateTime.now().getYear

lazy val tenseiAgent = project
  .in(file("."))
  .settings(settings)
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*)
  .settings(
    name := "tensei-agent",
    unmanagedBase := baseDirectory.value / "lib",
    fork in Test := true,
    parallelExecution in Test := false,
    fork in IntegrationTest := true,
    parallelExecution in IntegrationTest := false,
    libraryDependencies ++= Seq(
      databaseDriverLibrary.derby,
      databaseDriverLibrary.firebird,
      databaseDriverLibrary.h2,
      databaseDriverLibrary.hsqldb,
      databaseDriverLibrary.mariadb,
      databaseDriverLibrary.postgresql,
      databaseDriverLibrary.sqlite,
      library.aaltoXml,
      library.akkaCluster,
      library.akkaClusterMetrics,
      library.akkaClusterTools,
      library.akkaSlf4j,
      library.alpakkaFtp,
      library.c3p0,
      library.commonsMath,
      library.commonsNet,
      library.dfasdlCore,
      library.dfasdlUtils,
      library.guava,
      library.httpClient,
      library.jSch,
      library.logbackClassic,
      library.poi,
      library.poiExamples,
      library.tenseiApi,
      library.zeroAllocHash,
      library.akkaTestkit               % IntegrationTest,
      library.akkaTestkit               % Test,
      library.easyMock                  % IntegrationTest,
      library.easyMock                  % Test,
      library.edDsa                     % IntegrationTest,
      library.edDsa                     % Test,
      library.ftpletApi                 % IntegrationTest,
      library.ftpletApi                 % Test,
      library.ftpServerCore             % IntegrationTest,
      library.ftpServerCore             % Test,
      library.jettyWebApp               % IntegrationTest,
      library.jettyWebApp               % Test,
      library.jettyTestHelper           % IntegrationTest,
      library.jettyTestHelper           % Test,
      library.minaCore                  % IntegrationTest,
      library.minaCore                  % Test,
      library.scalaCheck                % IntegrationTest,
      library.scalaCheck                % Test,
      library.scalaTest                 % IntegrationTest,
      library.scalaTest                 % Test,
      library.sshd                      % IntegrationTest,
      library.sshd                      % Test,
      library.xmlUnit                   % IntegrationTest,
      library.xmlUnit                   % Test
    )
  )
  .enablePlugins(AutomateHeaderPlugin, GitBranchPrompt, GitVersioning)
  .enablePlugins(JavaServerAppPackaging, JDebPackaging, SystemVPlugin)
  .settings(
    daemonUser := "tensei-agent",
    daemonGroup := "tensei-agent",
    maintainer := "Wegtam GmbH <devops@wegtam.com>",
    packageSummary := "Tensei-Data Agent",
    packageDescription := "The tensei agent is the workhorse of a Tensei-Data system.",
    defaultLinuxInstallLocation := "/opt",
    debianPackageProvides in Debian += "tensei-agent",
    debianPackageDependencies in Debian += "openjdk-8-jre-headless",
    debianPackageRecommends in Debian ++= Seq("libhyperic-sigar-java", "tensei-server"),
    maintainerScripts in Debian := maintainerScriptsAppend((maintainerScripts in Debian).value)(
      DebianConstants.Postinst -> List(
        s"touch ${defaultLinuxInstallLocation.value}/${normalizedName.value}/tensei-agent-id.properties",
        s"chown ${daemonUser.value} ${defaultLinuxInstallLocation.value}/${normalizedName.value}/tensei-agent-id.properties"
      ).mkString(" && ")
    ),
    maintainerScripts in Debian := maintainerScriptsAppend((maintainerScripts in Debian).value)(
      DebianConstants.Postinst -> s"restartService ${normalizedName.value}"
    ),
    maintainerScripts in Debian := maintainerScriptsAppend((maintainerScripts in Debian).value)(
      DebianConstants.Postinst -> List(
        "mkdir -p /srv/tensei/data",
        s"chown ${daemonUser.value} /srv/tensei/data"
      ).mkString(" && ")
    ),
    requiredStartFacilities in Debian := Option("$local_fs $remote_fs $network"),
    // Disable packaging of api-docs.
    publishArtifact in (Compile, packageDoc) := false,
    publishArtifact in packageDoc := false,
    sources in (Compile, doc) := Seq.empty,
    // Package our configuration files into an extra directory.
    // FIXME There has to be a sane way to do this!
    mappings in Universal += new File(targetDirectory.value + "/logback.xml") -> "conf/logback.xml",
    mappings in Universal += new File(targetDirectory.value + "/application.conf") -> "conf/application.conf",
    mappings in Universal += new File(targetDirectory.value + "/tensei.conf") -> "conf/tensei.conf",
    // Require tests to be run before building a debian package.
    packageBin in Debian := ((packageBin in Debian) dependsOn (test in Test)).value
  )

lazy val targetDirectory = Def.setting(target.value + "/scala-" + scalaVersion.value.substring(0, scalaVersion.value.indexOf(".", 2)) + "/classes")

// A seperate project for benchmarks.
lazy val benchmarks = project
  .in(file("benchmarks"))
  .settings(settings)
  .settings(
    name := "benchmarks",
    libraryDependencies ++= Seq(
      library.jai,
      library.jamm
    ),
    javaOptions ++= (dependencyClasspath in Test).map(makeAgentOptions).value,
    javaOptions in run ++= List("-Xmx6g", "-XX:MaxMetaspaceSize=2g"),
    fork := true
  )
  .dependsOn(tenseiAgent)
  .enablePlugins(AutomateHeaderPlugin, GitBranchPrompt, GitVersioning, JmhPlugin)

/**
  * Helper function to generate options for instrumenting memory analysis.
  *
  * @param cp The current classpath.
  * @return A list of options (strings).
  */
def makeAgentOptions(cp: Classpath): Seq[String] = {
  val jammJar = cp.map(_.data).filter(_.toString.contains("jamm")).head
  val jaiJar = cp.map(_.data).filter(_.toString.contains("instrumenter")).head
  Seq(s"-javaagent:$jammJar", s"-javaagent:$jaiJar")
}

// *****************************************************************************
// Library dependencies
// *****************************************************************************

lazy val library =
  new {
    object Version {
      val aaltoXml        = "0.9.11"
      val akka            = "2.4.17"
      val alpakkaFtp      = "0.11"
      val c3p0            = "0.9.5.2"
      val commonsMath     = "3.6.1"
      val commonsNet      = "3.6"
      val dfasdlCore      = "1.0"
      val dfasdlUtils     = "2.0.0"
      val easyMock        = "3.4"
      val edDsa           = "0.2.0"
      val ftpServer       = "1.1.1"
      val guava           = "21.0"
      val httpClient      = "4.5"
      val jai             = "3.0.1"
      val jamm            = "0.3.1"
      val jettyWebApp     = "9.3.0.v20150612"
      val jettyTestHelper = "2.9"
      val jSch            = "0.1.54"
      val logback         = "1.1.11"
      val mina            = "2.0.16"
      val poi             = "3.15"
      val sigar           = "1.7.3"
      val scalaCheck      = "1.13.5"
      val scalaTest       = "3.0.4"
      val shapeless       = "2.3.2"
      val sshd            = "1.6.0"
      val tenseiApi       = "1.92.0"
      val xmlUnit         = "2.3.0"
      val zeroAllocHash   = "0.8"
    }
    val aaltoXml           = "com.fasterxml"               %  "aalto-xml"               % Version.aaltoXml
    val akkaCluster        = "com.typesafe.akka"           %% "akka-cluster"            % Version.akka
    val akkaClusterMetrics = "com.typesafe.akka"           %% "akka-cluster-metrics"    % Version.akka
    val akkaClusterTools   = "com.typesafe.akka"           %% "akka-cluster-tools"      % Version.akka
    val akkaSlf4j          = "com.typesafe.akka"           %% "akka-slf4j"              % Version.akka
    val akkaTestkit        = "com.typesafe.akka"           %% "akka-testkit"            % Version.akka
    val alpakkaFtp         = "com.lightbend.akka"          %% "akka-stream-alpakka-ftp" % Version.alpakkaFtp
    val c3p0               = "com.mchange"                 %  "c3p0"                    % Version.c3p0
    val commonsMath        = "org.apache.commons"          %  "commons-math3"           % Version.commonsMath
    val commonsNet         = "commons-net"                 %  "commons-net"             % Version.commonsNet
    val dfasdlCore         = "org.dfasdl"                  %% "dfasdl-core"             % Version.dfasdlCore
    val dfasdlUtils        = "org.dfasdl"                  %% "dfasdl-utils"            % Version.dfasdlUtils
    val easyMock           = "org.easymock"                %  "easymock"                % Version.easyMock
    val edDsa              = "net.i2p.crypto"              %  "eddsa"                   % Version.edDsa
    val ftpletApi          = "org.apache.ftpserver"        %  "ftplet-api"              % Version.ftpServer
    val ftpServerCore      = "org.apache.ftpserver"        %  "ftpserver-core"          % Version.ftpServer
    val guava              = "com.google.guava"            %  "guava"                   % Version.guava
    val httpClient         = "org.apache.httpcomponents"   %  "httpclient"              % Version.httpClient
    val jai = "com.google.code.java-allocation-instrumenter" % "java-allocation-instrumenter" % Version.jai
    val jamm               = "com.github.jbellis"          % "jamm"                     % Version.jamm
    val jettyWebApp        = "org.eclipse.jetty"           %  "jetty-webapp"            % Version.jettyWebApp
    val jettyTestHelper    = "org.eclipse.jetty.toolchain" %  "jetty-test-helper"       % Version.jettyTestHelper
    val jSch               = "com.jcraft"                  %  "jsch"                    % Version.jSch
    val logbackClassic     = "ch.qos.logback"              %  "logback-classic"         % Version.logback
    val minaCore           = "org.apache.mina"             %  "mina-core"               % Version.mina
    val poi                = "org.apache.poi"              %  "poi"                     % Version.poi
    val poiExamples        = "org.apache.poi"              %  "poi-examples"            % Version.poi
    val scalaCheck         = "org.scalacheck"              %% "scalacheck"              % Version.scalaCheck
    val scalaTest          = "org.scalatest"               %% "scalatest"               % Version.scalaTest
    val shapeless          = "com.chuusai"                 %% "shapeless"               % Version.shapeless
    val sigar              = "org.hyperic"                 %  "sigar"                   % Version.sigar
    val sshd               = "org.apache.sshd"             %  "sshd-core"               % Version.sshd
    val tenseiApi          = "com.wegtam.tensei"           %% "tensei-api"              % Version.tenseiApi
    val xmlUnit            = "org.xmlunit"                 %  "xmlunit-core"            % Version.xmlUnit
    val zeroAllocHash      = "net.openhft"                 %  "zero-allocation-hashing" % Version.zeroAllocHash
  }

lazy val databaseDriverLibrary =
  new {
    object Version {
      val derby      = "10.13.1.1"
      val firebird   = "2.2.12"
      val h2         = "1.4.193"
      val hsqldb     = "2.3.4"
      val mariadb    = "1.5.9"
      val postgresql = "42.1.4"
      val oracle     = "7"
      val sqlite     = "3.16.1"
      val sqlserver  = "4.1"
    }
    val derby      = "org.apache.derby"        % "derby"               % Version.derby
    val firebird   = "org.firebirdsql.jdbc"    % "jaybird-jdk18"       % Version.firebird
    val h2         = "com.h2database"          % "h2"                  % Version.h2
    val hsqldb     = "org.hsqldb"              % "hsqldb"              % Version.hsqldb
    val mariadb    = "org.mariadb.jdbc"        % "mariadb-java-client" % Version.mariadb
    val postgresql = "org.postgresql"          % "postgresql"          % Version.postgresql
    val oracle     = "oracle"                  % "jdbc"                % Version.oracle
    val sqlite     = "org.xerial"              % "sqlite-jdbc"         % Version.sqlite
    val sqlserver  = "com.microsoft.sqlserver" % "jdbc"                % Version.sqlserver
  }

// *****************************************************************************
// Settings
// *****************************************************************************

lazy val settings =
  commonSettings ++
  resolverSettings ++
  scalafmtSettings

lazy val commonSettings =
  Seq(
    headerLicense := Some(HeaderLicense.AGPLv3(s"2014 - $currentYear", "Contributors as noted in the AUTHORS.md file")),
    organization := "com.wegtam.tensei",
    git.useGitDescribe := true,
    scalaVersion in ThisBuild := "2.11.11",
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-target:jvm-1.8",
      "-unchecked",
      "-Xfatal-warnings",
      "-Xfuture",
      "-Xlint",
      "-Ydelambdafy:method",
      "-Ybackend:GenBCode",
      "-Yno-adapted-args",
      "-Ywarn-numeric-widen",
      "-Ywarn-unused-import",
      "-Ywarn-value-discard"
    ),
    javacOptions ++= Seq(
      "-encoding", "UTF-8",
      "-source", "1.8",
      "-target", "1.8"
    ),
    //wartremoverWarnings in (Compile, compile) ++= Warts.unsafe
  )

lazy val resolverSettings =
  Seq(
    resolvers += Resolver.bintrayRepo("wegtam", "dfasdl"),
    resolvers += Resolver.bintrayRepo("wegtam", "tensei-data")
  )

lazy val scalafmtSettings =
  Seq(
    scalafmtOnCompile := true,
    scalafmtOnCompile.in(Sbt) := false,
    scalafmtVersion := "1.2.0"
  )
// Enable scalafmt for the IntegrationTest scope.
inConfig(IntegrationTest)(com.lucidchart.sbt.scalafmt.ScalafmtCorePlugin.autoImport.scalafmtSettings)

