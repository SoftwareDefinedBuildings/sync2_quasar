package main

import (
    "fmt"
    "gopkg.in/mgo.v2"
    "github.com/SoftwareDefinedBuildings/sync2_quasar/parser"
    "net"
    "os"
    "sync"
    "time"
    capnp "github.com/glycerine/go-capnproto"
    cpint "github.com/SoftwareDefinedBuildings/quasar/cpinterface"
	uuid "code.google.com/p/go-uuid/uuid"
)

func main() {
    args := os.Args[1:]
    if len(args) != 14 {
        fmt.Println("14 arguments are required.")
        fmt.Println("The first argument is the serial number of the uPMU we are inserting data for.")
        fmt.Println("The remaining 13 arguments are the UUIDs of the streams for the uPMUs.")
        fmt.Println("They must be in the following order:")
        fmt.Println("L1 Magnitude")
        fmt.Println("L1 Angle")
        fmt.Println("L2 Magnitude")
        fmt.Println("L2 Angle")
        fmt.Println("L3 Magnitude")
        fmt.Println("L3 Angle")
        fmt.Println("C1 Magnitude")
        fmt.Println("C1 Angle")
        fmt.Println("C2 Magnitude")
        fmt.Println("C2 Angle")
        fmt.Println("C3 Magnitude")
        fmt.Println("C3 Angle")
        fmt.Println("Lock State")
        return
    }
    var serial_number string = args[0]
    var uuids = make([][]byte, NUM_STREAMS)
    var connections = make([]net.Conn, NUM_STREAMS)
    var sendLocks = make([]*sync.Mutex, NUM_STREAMS)
    var recvLocks = make([]*sync.Mutex, NUM_STREAMS)
    
    var i int
    
    for i = 0; i < NUM_STREAMS; i++ {
        uuids[i] = uuid.Parse(args[i + 1])
        sendLocks[i] = &sync.Mutex{}
        recvLocks[i] = &sync.Mutex{}
    }
    
    var err error
    for i = 0; i < NUM_STREAMS; i++ {
        connections[i], err = net.Dial("tcp", DB_ADDR)
        if err != nil {
            fmt.Printf("Error connecting to the database: %v\n", err)
            return
        }
    }
    
    session, err := mgo.Dial("localhost:27017")
    c := session.DB("upmu_database").C("received_files")
    //res := make(map[string]interface{})
    //err2 := c.Find(make(map[string]interface{})).One(&res)
    //fmt.Println(err)
    //fmt.Println(err2)
    //fmt.Printf("%T\n", res["data"])
    
    //var data []uint8 = res["data"].([]uint8)
    //var parsed []*parser.Sync_Output = parser.ParseSyncOutArray(data)
    //fmt.Println(*parsed[0])
    
    process_loop(c, serial_number, uuids, connections, sendLocks, recvLocks)
    
    session.Close()
}

// 120 points in each sync_output
const POINTS_PER_MESSAGE int = 120
const DB_ADDR string = "localhost:4410"
const NUM_STREAMS int = 13

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

const ytagbase int = 1

func insert_stream(uuid []byte, output *parser.Sync_Output, getValue func (int, *parser.Sync_Output) float64, startTime int64, connection net.Conn, sendLock *sync.Mutex, recvLock *sync.Mutex, feedback chan int) {
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
    
    var sendErr error
    sendLock.Lock()
    _, sendErr = segment.WriteTo(connection)
    sendLock.Unlock()
    
    insertPool.Put(mp)
    
    if sendErr != nil {
        fmt.Printf("Error in sending message: %v\n", sendErr)
        feedback <- 1
        return
    }
    feedback <- 0
    
    recvLock.Lock()
    responseSegment, respErr := capnp.ReadFromStream(connection, nil)
    recvLock.Unlock()
	
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

func process(coll *mgo.Collection, query map[string]interface{}, sernum string, uuids [][]byte, connections []net.Conn, sendLocks []*sync.Mutex, recvLocks []*sync.Mutex) {
    var documents *mgo.Iter = coll.Find(query).Iter()
    
    var result map[string]interface{} = make(map[string]interface{})
    
    var continueIteration bool = documents.Next(&result)
    
    var parsed []*parser.Sync_Output
    var synco *parser.Sync_Output
    var timeArr [6]int32
    var i int
    var j int
    var timestamp int64
    var feedback chan int
    var success bool
    var err error
    
    for continueIteration {
        success = true
        feedback = make(chan int)
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
            for j = 0; j < NUM_STREAMS; j++ {
                go insert_stream(uuids[j], synco, insertGetters[j], timestamp, connections[j], sendLocks[j], recvLocks[j], feedback)
            }
        }
        for j = 0; j < NUM_STREAMS; j++ {
            if <-feedback == 1 {
                fmt.Println("Warning: data for a stream could not be sent")
                success = false
            }
        }
        if success {
            err = coll.Update(map[string]interface{}{
                "_id": result["_id"],
            }, map[string]interface{}{
                "$set": map[string]interface{}{
                    "ytag": ytagbase,
                },
            })
    
            if err != nil {
                fmt.Printf("Could not update ytag: %v\n", err)
            }
        }
        continueIteration = documents.Next(&result)
    }
    
    err = documents.Err()
    if err != nil {
        fmt.Printf("Could not iterate through documents: %v\n", err)
    }
    
    return
}

func process_loop(coll *mgo.Collection, sernum string, uuids [][]byte, connections []net.Conn, sendLocks []*sync.Mutex, recvLocks []*sync.Mutex) {
    query := map[string]interface{}{
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
    for true {
        fmt.Println("looping")
        process(coll, query, sernum, uuids, connections, sendLocks, recvLocks)
        fmt.Println("sleeping")
        time.Sleep(time.Second)
        break // for now, we break. We'll eventually remove this.
    }
}
