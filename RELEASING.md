How to release
--------------

- create an annotated tag for the next version. EG: `git tag -a -m 'release v1.3.1' v1.3.1`
- push tags and commits to akka remote master
- If the workspace is not clean or HEAD != the tag then published version will be a snapshot. To use
  exactly the tagged version the workspace must be clean and there are no additional commits beyond
  the tag.
- `sbt -Dpublish.maven.central=true +publishSigned`
- Travis CI will update the `akka-persistence-dynamodb-xx-stable` reporting project in
  [WhiteSource](http://saas.whitesourcesoftware.com/) on push to master.
  The `xx` is the `major.minor` of the current version. This needs a manual update when
  the minor version is incremented.
