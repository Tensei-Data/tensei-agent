addSbtPlugin("com.eed3si9n"       % "sbt-buildinfo"       % "0.7.0")
addSbtPlugin("com.typesafe.sbt"   % "sbt-git"             % "0.9.3")
addSbtPlugin("de.heikoseeberger"  % "sbt-header"          % "2.0.0")
addSbtPlugin("pl.project13.scala" % "sbt-jmh"             % "0.2.27")
addSbtPlugin("com.typesafe.sbt"   % "sbt-native-packager" % "1.2.2")
addSbtPlugin("com.jsuereth"       % "sbt-pgp"             % "1.1.0")
addSbtPlugin("com.lucidchart"     % "sbt-scalafmt"        % "1.10")
addSbtPlugin("org.scoverage"      % "sbt-scoverage"       % "1.5.0")
addSbtPlugin("org.wartremover"    % "sbt-wartremover"     % "2.2.0")
// Needed to build debian packages via java (for sbt-native-packager).
libraryDependencies += "org.vafer" % "jdeb" % "1.5" artifacts Artifact("jdeb", "jar", "jar")

