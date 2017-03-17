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
	"os"
	"log"
	"strings"
	"time"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"github.com/vaelen/mongo.utils/sharding"
)

func main() {
	url := "mongodb://localhost"
	if len(os.Args) > 1 {
		url = os.Args[1]
	}
	dialInfo, err := mgo.ParseURL(url)
	if err != nil {
		log.Fatalf("Could not parse MongoDB URL: %s\n", err.Error())
	}

	SimpleCount(dialInfo)
	
}

func SimpleCount(dialInfo *mgo.DialInfo) {
	mongosSession, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		log.Fatalf("Could not connect to MongoDB instance: %s\n", err.Error())
	}
	defer mongosSession.Close()
	
	collections, err := sharding.Collections(mongosSession)
	if err != nil {
		log.Fatalf("Could not retrieve the list of available collections: %s\n", err.Error())
	}
	for _, c := range collections {
		log.Printf("Looking for Orphans.  DB Name: %s\n", c.NS);

		docCount, err := CountDocumentsOnShard(mongosSession, c.NS)
		if err != nil {
			log.Fatalf("Error counting documents for %s: %s\n", c.NS, err.Error())
		}
		realCount, err := CountRealDocumentsOnShard(mongosSession, c.NS)
		if err != nil {
			log.Fatalf("Error counting actual documents for %s: %s\n", c.NS, err.Error())
		}
		orphans := 0
		if docCount > realCount {
			orphans = docCount - realCount
		}
		log.Printf("DB Name: %s, Document Count: %d, Actual Documents: %d, Orphans: %d\n", c.NS, docCount, realCount, orphans)
	}
}

// NOTE: This doesn't work properly yet
func AdvancedCount(dialInfo *mgo.DialInfo) {
	mongosSession, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		log.Fatalf("Could not connect to MongoDB instance: %s\n", err.Error())
	}
	defer mongosSession.Close()

	shards, err := sharding.Shards(mongosSession)
	if err != nil {
		log.Fatalf("Could not retrieve the list of available shards: %s\n", err.Error())
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
		s, err := mgo.DialWithInfo(shardDialInfo)
		if err != nil {
			log.Fatalf("Could not connect to shard %s (%s): %s\n", shard.Name, shard.Host, err.Error())
		}
		defer s.Close()
		shardSessions[shard.Name] = s
	}
	
	collections, err := sharding.Collections(mongosSession)
	if err != nil {
		log.Fatalf("Could not retrieve the list of available collections: %s\n", err.Error())
	}
	for _, c := range collections {
		log.Printf("Looking for Orphans.  DB Name: %s\n", c.NS);

		totalCount, m, err := CountOrphans(mongosSession, shardSessions, c.NS)
		if err != nil {
			log.Fatalf("Error Counting Orphans for %s: %s\n", c.NS, err.Error())
		}
		log.Printf("DB Name: %s, Document Count: %d\n", c.NS, totalCount)
		for n, oc := range m {
			log.Printf("DB Name: %s, Shard: %s, Document Count: %d, Actual: %d, Orphans: %d\n",
				c.NS, n, oc.Docs, oc.RealDocs, oc.Orphans())
		}
	}
}

type OrphanCount struct {
	Docs int
	RealDocs int
}

func (oc OrphanCount) HasOrphans() bool {
	return oc.RealDocs > oc.Docs
}

func (oc OrphanCount) Orphans() int {
	if oc.HasOrphans() {
		return oc.RealDocs - oc.Docs
	}
	return 0
}

func SplitNS(ns string) (string, string) {
	nsSplice := strings.SplitN(ns, ".", 2)
	return nsSplice[0], nsSplice[1]
}	

func CountOrphans(mongosSession *mgo.Session, shardSessions map[string]*mgo.Session, ns string) (int, map[string]OrphanCount, error) {
	m := make(map[string]OrphanCount)
	c, err := CountRealDocumentsOnShard(mongosSession, ns)
	if err != nil {
		return -1, nil, err
	}
	for n, s := range shardSessions {
		shardCount, err := CountOrphansOnShard(mongosSession, s, ns, n)
		if err != nil {
			return -1, nil, err
		}
		m[n] = shardCount
	}
	return c, m, nil
}


func CountOrphansOnShard(mongos *mgo.Session, shard *mgo.Session, ns string, shardName string) (OrphanCount, error){
	count, err := CountDocumentsOnShard(shard, ns)
	if err != nil {
		return OrphanCount{}, err
	}
	
	realCount, err := CountRealDocumentsOnShard(shard, ns)
	if err != nil {
		return OrphanCount{}, err
	}

	return OrphanCount{Docs: count, RealDocs: realCount}, nil
}

func CountChunkDocumentsOnShard(mongos *mgo.Session, shard *mgo.Session, ns string, shardName string) (int, error) {
	dbName, colName := SplitNS(ns)
	chunks, err := sharding.ChunksForNSAndShard(mongos, ns, shardName)
	if err != nil {
		return -1, err
	}
	counts := make([]int, 0, len(chunks))
	scounts := make([]int, 0, len(chunks))
	log.Printf("Shard: %s, Chunks: %d\n", shardName, len(chunks))
	for _, chunk := range chunks {
		q := bson.D{}

		min := chunk.Min.Map()
		max := chunk.Max.Map()
		
		// Build a query that includes chunk.Min and excludes chunk.Max
		for key, minValue := range min {
			maxValue := max[key]
			qMin := bson.DocElem{ Name: "$gte", Value: minValue }
			qMax := bson.DocElem{ Name: "$lt", Value: maxValue }
			qv := bson.D { qMin, qMax }
			q = append(q, bson.DocElem{ Name: key, Value: qv })
		}

		c, err := shard.DB(dbName).C(colName).Find(q).Count()
		if err != nil {
			return -1, err
		}

		counts = append(counts, c)
		//log.Printf("Shard: %s, Chunk Count: %d, Total Count: %d\n", shardName, c, count)

		sc, err := mongos.DB(dbName).C(colName).Find(q).Count()
		if err != nil {
			return -1, err
		}

		scounts = append(scounts, sc)
	}
	//log.Printf("Counts: %v\n", counts)
	totalCount := sum(counts)
	log.Printf("%s Total Count: %d\n", shardName, totalCount)
	//log.Printf("mongos Counts: %v\n", scounts)
	totalSCount := sum(scounts)
	log.Printf("mongos Total Count: %d\n", totalSCount)

	return totalCount, nil
}

func sum(counts []int) int {
	totalCount := 0
	for _, count := range counts {
		totalCount += count
	}
	return totalCount
}

func CountRealDocumentsOnShard(session *mgo.Session, ns string) (int, error) {
	dbName, colName := SplitNS(ns)
	db := session.DB(dbName)
	count := 0
	iter := db.C(colName).Find(bson.M{}).Select(bson.M{"_id": 1}).Iter()
	result := bson.D{}
	for iter.Next(&result) {
		count++
	}
	err := iter.Close()
	if err != nil {
		return -1, err
	}
	return count, nil
}

func CountDocumentsOnShard(session *mgo.Session, ns string) (int, error) {
	dbName, colName := SplitNS(ns)
	db := session.DB(dbName)
	return db.C(colName).Count()
}

