// Mongo.Utils, A set of MongoDB utilities written in Go
// Copyright (C) 2017 Andrew Young <andrew@vaelen.org>
// 
//     This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
//     This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
//     You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"log"
	"strings"
	"time"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Chunk struct {
	NS string "ns"
	Min bson.D "min"
	Max bson.D "max"
}

type Collection struct {
	NS string "_id"
	Dropped bool
	Unique bool
	Key bson.D
}

type Shard struct {
	Name string "_id"
	Host string
	State int
	Tags []string
}

func main() {
	url := "mongodb://localhost:30000"
	dialInfo, err := mgo.ParseURL(url)
	if err != nil {
		log.Fatalf("Could not parse MongoDB URL: %s\n", err.Error())
	}
	
	mongosSession, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		log.Fatalf("Could not connect to MongoDB instance: %s\n", err.Error())
	}
	defer mongosSession.Close()

	configDb := mongosSession.DB("config")

	shards, err := GetShards(mongosSession)
	if err != nil {
		log.Fatalf("Could not determine the list of available shards: %s\n", err.Error())
	}

	shardSessions := make(map[string]*mgo.Session)
	for _, shard := range shards {
		shardDialInfo, err := mgo.ParseURL(shard.Host)
		if err != nil {
			log.Fatalf("Could not parse shard URL (%s): %s\n", shard.Host, err.Error())
		}
		addrs := shardDialInfo.Addrs
		*shardDialInfo = *dialInfo
		shardDialInfo.Addrs = addrs
		if shardDialInfo.Timeout == 0 {
			shardDialInfo.Timeout = 2 * time.Second
		}
		log.Printf("Connecting to %s (%s)\n", shard.Name, shard.Host)
		log.Println(dialInfo)
		log.Println(shardDialInfo)
		s, err := mgo.DialWithInfo(shardDialInfo)
		if err != nil {
			log.Fatalf("Could not connect to shard %s (%s): %s\n", shard.Name, shard.Host, err.Error())
		}
		defer s.Close()
		shardSessions[shard.Name] = s
	}
	
	collections := make([]Collection,0)
	configDb.C("collections").Find(bson.M{"dropped": false}).All(&collections)

	for _, c := range collections {
		_, _, err := CountOrphans(mongosSession, shardSessions, c.NS)
		if err != nil {
			log.Fatalf("Error Counting Orphans for %s: %s\n", c.NS, err.Error())
		}
	}
}

func CountOrphans(mongosSession *mgo.Session, shardSessions map[string]*mgo.Session, ns string) (int, map[string]int, error) {
	nsSplice := strings.SplitN(ns, ".", 2)
	dbName := nsSplice[0]
	colName := nsSplice[1]
	log.Printf("Looking for Orphans.  DB Name: %s, Collection Name: %s\n", dbName, colName);
	m := make(map[string]int)
	c, err := CountDocumentsOnShard(mongosSession, dbName, colName)
	if err != nil {
		return -1, nil, err
	}
	message := "DB Name: %s, Collection Name: %s, Shard Name: %s, Count: %d\n"
	log.Printf(message, dbName, colName, "mongos", c)
	for n, s := range shardSessions {
		shardCount, err := CountDocumentsOnShard(s, dbName, colName)
		if err != nil {
			return -1, nil, err
		}
		log.Printf(message, dbName, colName, n, c)
		m[n] = shardCount
	}
	return c, m, nil
}

func CountDocumentsOnShard(session *mgo.Session, dbName string, colName string) (int, error) {
	db := session.DB(dbName)
	return db.C(colName).Count()
}

func GetShards(session *mgo.Session) ([]Shard, error) {
	results := make([]Shard, 0)
	configDb := session.DB("config")
	err := configDb.C("shards").Find(bson.M{}).All(&results)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func ListChunks(configDb *mgo.Database) {
	iter := configDb.C("chunks").Find(bson.M{}).Iter()
	chunk := Chunk{}
	for iter.Next(&chunk) {
		log.Println("Chunk:", chunk)
	}
	
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
}
