How to release
--------------

- edit the `version.sbt` file
- commit
- tag it as `vX.Y.Z`
- push things
- `sbt -Dpublish.maven.central=true ++publishSigned`
- Travis CI will update the `akka-persistence-dynamodb-xx-stable` reporting project in
  [WhiteSource](http://saas.whitesourcesoftware.com/) on push to master.
