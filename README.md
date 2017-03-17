# Mongo.Utils
A set of MongoDB utilities written in Go

## connectToMongoDB

This application does exactly what it says.  Given a MongoDB URL it attempts to connect to it.

To install, simply run:
``` sh
go get github.com/vaelen/mongo.utils/connectToMongoDB
```

Here is an example of using this application:
```sh
$ connectToMongoDB 'mongodb://localhost:30001'
2017/03/17 18:17:41 Connecting to host(s): [localhost:30001]
2017/03/17 18:17:41 Successfully connected
```

## countOrphans

This application attempts to count the number of orphan documents in each of your sharded collections.  The only argument is the MongoDB URL to use when connecting to the cluster.  This URL should point to a mongos instance.

At the moment this application uses the simplest method to count orphans, which will tell you how many orphans you have but not which servers they are located on.  A more complex algorithm is in the works.

**Note:** This application will read every document in each of your sharded collections.  This will likely cause a lot of IO load on your nodes.

To install, simply run:
``` sh
go get github.com/vaelen/mongo.utils/countOrphans
```

Here is an example of using this application:
``` sh
$ countOrphans 'mongodb://localhost:30000'
2017/03/17 18:17:11 Looking for Orphans.  DB Name: twitter.tweets
2017/03/17 18:17:18 DB Name: twitter.tweets, Document Count: 1369028, Actual Documents: 1369027, Orphans: 1
2017/03/17 18:17:18 Looking for Orphans.  DB Name: twitter.raw
2017/03/17 18:17:31 DB Name: twitter.raw, Document Count: 1372828, Actual Documents: 1369027, Orphans: 3801
```
