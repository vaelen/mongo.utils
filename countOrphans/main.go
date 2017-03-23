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
	"sort"
	"time"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"github.com/vaelen/mongo.utils/sharding"
)

const UseSimpleCount = false

func main() {
	url := "mongodb://localhost"
	if len(os.Args) > 1 {
		url = os.Args[1]
	}
	dialInfo, err := mgo.ParseURL(url)
	if err != nil {
		log.Fatalf("Could not parse MongoDB URL: %s\n", err.Error())
	}

	//SimpleCount(dialInfo)
	AdvancedCount(dialInfo)
	
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
			shardDialInfo.Timeout = 10 * time.Second
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

		mongosResults, results, err := CountOrphans(mongosSession, shardSessions, c.NS)
		if err != nil {
			log.Fatalf("Error Counting Orphans for %s: %s\n", c.NS, err.Error())
		}
		log.Printf("DB Name: %s, Document Count: %d, Actual: %d, Orphans: %d\n",
			c.NS, mongosResults.DocCount, mongosResults.ActualDocCount, mongosResults.Orphans())
		for _, shardName := range sortedKeys(results) {
			oc := results[shardName]
			log.Printf("DB Name: %s, Shard: %s, Document Count: %d, Actual: %d, Orphans: %d\n",
				c.NS, shardName, oc.DocCount, oc.ActualDocCount, oc.Orphans())
		}
	}
}

func sortedKeys(m map[string]OrphanCount) []string {
	keys := make([]string, 0, len(m))
	for key,_ := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

type OrphanCount struct {
	Shard string
	DocCount int
	ActualDocCount int
	Error error
}

func (oc OrphanCount) HasOrphans() bool {
	return oc.DocCount > oc.ActualDocCount
}

func (oc OrphanCount) Orphans() int {
	if oc.HasOrphans() {
		return oc.DocCount - oc.ActualDocCount
	}
	return 0
}

func SplitNS(ns string) (string, string) {
	nsSplice := strings.SplitN(ns, ".", 2)
	return nsSplice[0], nsSplice[1]
}	

func CountOrphans(mongosSession *mgo.Session, shardSessions map[string]*mgo.Session, ns string) (OrphanCount, map[string]OrphanCount, error) {
	results := make(map[string]OrphanCount)
	messages := make(chan OrphanCount, len(shardSessions))
	count, err := CountDocumentsOnShard(mongosSession, ns)
	if err != nil {
		return OrphanCount{}, nil, err
	}
	actualCount, err := CountRealDocumentsOnShard(mongosSession, ns)
	if err != nil {
		return OrphanCount{}, nil, err
	}
	for shardName, shardSession := range shardSessions {
		go countOrphansOnShardWorker(mongosSession, shardSession, ns, shardName, messages)
	}
	for i := 0; i < len(shardSessions); i++ {
		oc := <-messages
		if oc.Error != nil {
			return OrphanCount{}, nil, oc.Error
		}
		results[oc.Shard] = oc
	}

	return OrphanCount{DocCount: count, ActualDocCount: actualCount}, results, nil
}

func countOrphansOnShardWorker(mongosSession *mgo.Session, shardSession *mgo.Session, ns string, shardName string, messages chan<- OrphanCount) {
	// log.Printf("Starting: %s", shardName)
	messages <- CountOrphansOnShard(mongosSession, shardSession, ns, shardName)
	// log.Printf("Finishing: %s", shardName)	
}

func CountOrphansOnShard(mongos *mgo.Session, shard *mgo.Session, ns string, shardName string) OrphanCount {
	var err error
	var actualCount int
	
	if UseSimpleCount {
		actualCount, err = CountRealDocumentsOnShard(shard, ns)
	} else {
		// NOTE: This doesn't work properly yet
		actualCount, err = CountChunkDocumentsOnShard(mongos, shard, ns, shardName)
	}
	if err != nil {
		return OrphanCount{Shard: shardName, Error: err}
	}
	
	count, err := CountDocumentsOnShard(shard, ns)
	if err != nil {
		return OrphanCount{Shard: shardName, Error: err}
	}

	return OrphanCount{Shard: shardName, DocCount: count, ActualDocCount: actualCount}
}

// NOTE: This probably works now.  I still need to verify some corner cases.
func CountChunkDocumentsOnShard(mongos *mgo.Session, shard *mgo.Session, ns string, shardName string) (int, error) {
	dbName, colName := SplitNS(ns)
	chunks, err := sharding.ChunksForNSAndShard(mongos, ns, shardName)
	if err != nil {
		return -1, err
	}
	counts := make([]int, 0, len(chunks))
	// scounts := make([]int, 0, len(chunks))
	// log.Printf("Shard: %s, Chunks: %d\n", shardName, len(chunks))
	for _, chunk := range chunks {
		var minQuery, maxQuery []bson.D
		
		// Build a query that includes chunk.Min and excludes chunk.Max
		minQuery, err = buildChunkClause(chunk.Min, "$gt", "$gte", bson.D{})
		if err != nil {
			return -1, err
		}

		maxQuery, err = buildChunkClause(chunk.Max, "$lt", "$lt", bson.D{})
		if err != nil {
			return -1, err
		}

		queries := make([]bson.M, 2)
		queries[0] = bson.M { "$or": minQuery }
		queries[1] = bson.M { "$or": maxQuery }
		
		q := bson.M{ "$and": queries }

		//log.Printf("Query: %v\n", q)
		
		c, err := shard.DB(dbName).C(colName).Find(q).Count()
		if err != nil {
			return -1, err
		}

		counts = append(counts, c)
		//log.Printf("Shard: %s, Chunk Count: %d, Total Count: %d\n", shardName, c, count)

		/*
		sc, err := mongos.DB(dbName).C(colName).Find(q).Count()
		if err != nil {
			return -1, err
		}

		scounts = append(scounts, sc)
        */
	}
	totalCount := sum(counts)
	// log.Printf("%s Total Count: %d\n", shardName, totalCount)

	/*
	totalSCount := sum(scounts)
	log.Printf("mongos Total Count: %d\n", totalSCount)
    */

	return totalCount, nil
}

func buildChunkClause(fields bson.D, op string, leafOp string, prevFields bson.D) ([]bson.D, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	// Get the next field off the list of fields
	field := fields[0]
	fields = fields[1:]

	if len(fields) == 0 {
		// No more fields after this one
		op = leafOp
	}
	
	// This is our computed clause: { ..., x: { $gt: 1 } }
	clause := append(prevFields, bson.DocElem{Name: field.Name, Value: bson.M{op: field.Value}})
	// This is used to build the next clause: { ..., x: 1 }
	nextFields := append(prevFields, field)

	// Build the next clause if there is one
	nextClause, err := buildChunkClause(fields, op, leafOp, nextFields)
	if err != nil {
		return nil, nil
	}

	// Build our return value
	
	clauses := make([]bson.D,1)
	clauses[0] = clause
	
	if nextClause != nil {
		for _, c := range nextClause {
			clauses = append(clauses, c)
		}
	}

	return clauses, nil
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

