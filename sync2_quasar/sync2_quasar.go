package main

import (
    "fmt"
    "gopkg.in/mgo.v2"
    "github.com/SoftwareDefinedBuildings/sync2_quasar/parser"
    "sync"
    "time"
    capnp "github.com/glycerine/go-capnproto"
    cpint "github.com/SoftwareDefinedBuildings/quasar/cpinterface"
	//uuid "code.google.com/p/go-uuid/uuid"
)

func main() {
    session, err := mgo.Dial("localhost:27017")
    c := session.DB("upmu_database").C("received_files")
    res := make(map[string]interface{})
    err2 := c.Find(make(map[string]interface{})).One(&res)
    fmt.Println(err)
    fmt.Println(err2)
    fmt.Printf("%T\n", res["data"])
    
    var data []uint8 = res["data"].([]uint8)
    var parsed []*parser.Sync_Output = parser.ParseSyncOutArray(data)
    fmt.Println(*parsed[0])
    
    session.Close()
}

// 120 points in each sync_output
const POINTS_PER_MESSAGE int = 120

type InsertMessagePart struct {
	segment *capnp.Segment
	request *cpint.Request
	insert *cpint.CmdInsertValues
	recordList *cpint.Record_List
	pointerList *capnp.PointerList
	record *cpint.Record
}

var insertPool sync.Pool = sync.Pool{
	New: func () interface{} {
		var seg *capnp.Segment = capnp.NewBuffer(nil)
		var req cpint.Request = cpint.NewRootRequest(seg)
		var insert cpint.CmdInsertValues = cpint.NewCmdInsertValues(seg)
		insert.SetSync(false)
		var recList cpint.Record_List = cpint.NewRecordList(seg, POINTS_PER_MESSAGE)
		var pointList capnp.PointerList = capnp.PointerList(recList)
		var record cpint.Record = cpint.NewRecord(seg)
		return InsertMessagePart{
			segment: seg,
			request: &req,
			insert: &insert,
			recordList: &recList,
			pointerList: &pointList,
			record: &record,
		}
	},
}

func insert_stream(uuid []byte, data *parser.Sync_Output) {
    // TODO insert the data into quasar
    return
}

func process() {
    // TODO check if there are any files in Mongo left to insert
    return
}

func process_loop() {
    for true {
        fmt.Println("looping")
        process()
        fmt.Println("sleeping")
        time.Sleep(time.Second)
    }
}
