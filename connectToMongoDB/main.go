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
	"time"
	"gopkg.in/mgo.v2"
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

	if dialInfo.Timeout == 0 {
		dialInfo.Timeout = 2 * time.Second
	}

	log.Printf("Connecting to host(s): %v\n", dialInfo.Addrs)
	
	mongosSession, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		log.Fatalf("Could not connect to MongoDB instance: %s\n", err.Error())
	}
	defer mongosSession.Close()
	log.Println("Successfully connected")
}
