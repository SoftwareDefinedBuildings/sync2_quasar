package main

import (
    "fmt"
    "gopkg.in/mgo.v2"
    "github.com/SoftwareDefinedBuildings/sync2_quasar/parser"
    "net"
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
    
    process(c, "hello")
    
    session.Close()
}

// 120 points in each sync_output
const POINTS_PER_MESSAGE int = 120

const DB_ADDR string = "localhost:4410"

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

var connectionPool sync.Pool = sync.Pool{
    New: func () interface{} {
        conn, err := net.Dial("tcp", DB_ADDR)
        if (err != nil) {
            fmt.Printf("Error connecting to database: %v\n", err)
        }
        return conn
    },
}

/** This function doesn't really use OUTPUT. It just is needed to GETVALUE can be called with the correct arguments. */
func insert_stream(uuid []byte, output *parser.Sync_Output, getValue func (int, *parser.Sync_Output) float64, startTime int64) {
    var mp InsertMessagePart = insertPool.Get().(InsertMessagePart)
    
    segment := mp.segment
	request := *mp.request
	insert := *mp.insert
	recordList := *mp.recordList
	pointerList := *mp.pointerList
	record := *mp.record
	
	request.SetEchoTag(0)
	
	insert.SetUuid(uuid)
	
	var timeDelta float64 = 1000000000 / float64(POINTS_PER_MESSAGE)
	for i := 0; i < POINTS_PER_MESSAGE; i++ {
	    record.SetTime(startTime + int64(float64(i) * timeDelta))
	    record.SetValue(getValue(i, output))
	    pointerList.Set(i, capnp.Object(record))
	}
	insert.SetValues(recordList)
	request.SetInsertValues(insert)
    
    var connection net.Conn = connectionPool.Get().(net.Conn)
    
    var sendErr error
    _, sendErr = segment.WriteTo(connection)
    
    insertPool.Put(mp)
    
    if sendErr != nil {
        fmt.Printf("Error in sending message: %v\n", sendErr)
        connectionPool.Put(connection)
        
        return
    }
    
    responseSegment, respErr := capnp.ReadFromStream(connection, nil)
	
	connectionPool.Put(connection)	
	if respErr != nil {
		fmt.Printf("Error in receiving response: %v\n", respErr)
		return
	}
	
	response := cpint.ReadRootResponse(responseSegment)
	status := response.StatusCode()
	if status != cpint.STATUSCODE_OK {
		fmt.Printf("Quasar returns status code %s!\n", status)
	}
	
    return
}

func process(coll *mgo.Collection, sernum string) {
    var ytagbase int = 1
    var query = map[string]interface{}{
        "serial_number": sernum,
        "xtag": map[string]bool{
            "$exists": false,
        },
        "$or": [2]map[string]interface{}{
            map[string]interface{}{
                "ytag": map[string]int{
                    "$lt": ytagbase,
                 },
            }, map[string]interface{}{
                "ytag": map[string]bool{
                    "$exists": false,
                },
            },
        },
    }
    
    var documents *mgo.Iter = coll.Find(query).Iter()
    
    var result map[string]interface{} = make(map[string]interface{})
    
    var continueIteration bool = documents.Next(&result)
    
    var parsed []*parser.Sync_Output
    var synco *parser.Sync_Output
    var timeArr [6]int32
    var i int
    var timestamp int64
    
    for continueIteration {
        parsed = parser.ParseSyncOutArray(result["data"].([]uint8))
        for i = 0; i < len(parsed); i++ {
            synco = parsed[i]
            timeArr = synco.Sync_Data.Times
            fmt.Printf("time: %v\n", timeArr)
            if timeArr[0] < 2010 || timeArr[0] > 2020 {
                // if the year is outside of this range things must have gotten corrupted somehow
                fmt.Printf("Rejecting bad date record: year is %v\n", timeArr[0])
                continue
            }
            timestamp = time.Date(int(timeArr[0]), time.Month(timeArr[1]), int(timeArr[2]), int(timeArr[3]), int(timeArr[4]), int(timeArr[5]), 0, time.UTC).UnixNano()
            fmt.Printf("timestamp: %v\n", timestamp)
        }
        continueIteration = documents.Next(&result)
    }
    
    var err error = documents.Err()
    if err != nil {
        fmt.Printf("Could not iterate through documents: %v\n", err)
    }
    
    return
}

func process_loop(coll *mgo.Collection, sernum string) {
    for true {
        fmt.Println("looping")
        process(coll, sernum)
        fmt.Println("sleeping")
        time.Sleep(time.Second)
    }
}
