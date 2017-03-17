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

package sharding

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Chunk struct {
	Id string "_id"
	NS string "ns"
	Min bson.D "min"
	Max bson.D "max"
	Shard string "shard"
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

func Shards(session *mgo.Session) ([]Shard, error) {
	results := make([]Shard, 0)
	err := session.DB("config").C("shards").Find(bson.M{}).All(&results)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func Collections(session *mgo.Session) ([]Collection, error) {
	results := make([]Collection, 0)
	err := session.DB("config").C("collections").Find(bson.M{}).All(&results)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func Chunks(session *mgo.Session) ([]Chunk, error) {
	return ChunksForQuery(session, bson.M{})
}

func ChunksIter(session *mgo.Session) *mgo.Iter {
	return ChunksIterForQuery(session, bson.M{})
}

func ChunksForQuery(session *mgo.Session, query interface{}) ([]Chunk, error) {
	results := make([]Chunk, 0)
	err := session.DB("config").C("chunks").Find(query).All(&results)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func ChunksIterForQuery(session *mgo.Session, query interface{}) *mgo.Iter {
	return session.DB("config").C("chunks").Find(bson.M{}).Iter()
}

func ChunksForNS(session *mgo.Session, ns string) ([]Chunk, error) {
	return ChunksForQuery(session, bson.M{"ns": ns})
}

func ChunksIterForNS(session *mgo.Session, ns string) *mgo.Iter {
	return ChunksIterForQuery(session, bson.M{"ns": ns})
}

func ChunksForNSAndShard(session *mgo.Session, ns string, shard string) ([]Chunk, error) {
	return ChunksForQuery(session, bson.M{"ns": ns, "shard": shard})
}

func ChunksIterForNSAndShard(session *mgo.Session, ns string, shard string) *mgo.Iter {
	return ChunksIterForQuery(session, bson.M{"ns": ns, "shard": shard})
}
