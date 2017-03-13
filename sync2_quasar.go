package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/SoftwareDefinedBuildings/sync2_quasar/configparser"
	"github.com/SoftwareDefinedBuildings/sync2_quasar/upmuparser"
	"gopkg.in/btrdb.v4"
	"gopkg.in/mgo.v2"

	uuid "github.com/pborman/uuid"
)

var btrdbconn *btrdb.BTrDB
var ytagbase int = 0
var configfile []byte = nil

func checkConfigFile() bool {
	var file *os.File
	var err error
	file, err = os.Open("upmuconfig.ini")
	if err != nil {
		fmt.Printf("Could not open upmuconfig.ini: %v\n", err)
		os.Exit(1)
	}

	defer file.Close()

	var fd uintptr = file.Fd()

	// Will block until acquired
	err = syscall.Flock(int(fd), syscall.LOCK_EX)
	if err != nil {
		fmt.Printf("WARNING: could not lock upmuconfig.ini: %v\n", err)
		return false
	}

	var filecontents []byte
	filecontents, err = ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("Could not read upmuconfig.ini: %v\n", err)
		os.Exit(1)
	}

	if len(filecontents) == 0 {
		fmt.Println("Configuration file (upmuconfig.ini) is empty!")
		os.Exit(1)
	}

	if configfile == nil || !bytes.Equal(filecontents, configfile) {
		configfile = filecontents
		return true
	}

	return false
}

func main() {
	var changed bool
	var err error

	changed = checkConfigFile()
	if !changed {
		fmt.Println("Could not read upmuconfig.ini")
		return
	}

	ctx := context.Background()

	btrdbconn, err = btrdb.Connect(ctx, btrdb.EndpointsFromEnv()...)
	if err != nil {
		fmt.Printf("Error connecting to the QUASAR database: %v\n", err)
		return
	}

	defer func() {
		err := btrdbconn.Disconnect()
		if err == nil {
			fmt.Println("Finished closing connection")
		} else {
			fmt.Printf("Could not close connection: %v\n", err)
		}
	}()

	var terminate bool = false

	var alive bool = true // if this were C I'd have to malloc this
	var interrupt = make(chan os.Signal)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		for {
			<-interrupt // block until an interrupt happens
			fmt.Println("\nDetected ^C. Waiting for pending tasks to complete...")
			alive = false
		}
	}()

	/* Start over if the configuration file changes */
	go func() {
		var changed bool = false
		for {
			time.Sleep(15 * time.Second)
			if checkConfigFile() {
				changed = true
			} else if changed {
				changed = false
				// start from scratch
				fmt.Println("Configuration file changed. Restarting...")
				terminate = false
				alive = false
			}
		}
	}()

	for !terminate {
		// If we die, just terminate (unless this is set differently)
		alive = true
		terminate = true

		config, isErr := configparser.ParseConfig(string(configfile))
		if isErr {
			fmt.Println("There were errors while parsing upmuconfig.ini. See above.")
			return
		}

		var syncconfigfile []byte
		syncconfigfile, err = ioutil.ReadFile("syncconfig.ini")
		if err != nil {
			fmt.Printf("Could not read syncconfig.ini: %v\n", err)
			return
		}

		syncconfig, isErr := configparser.ParseConfig(string(syncconfigfile))
		if isErr {
			fmt.Println("There were errors while parsing syncconfig.ini. See above.")
			return
		}

		runtime.GOMAXPROCS(runtime.NumCPU())

		var complete = make(chan bool)

		var num_uPMUs = 0
		var temp interface{}
		var serial string
		var alias string
		var ok bool
		var uuids []string
		var i int
		var streamMap map[string]interface{}
		var ip string
		var upmuMap map[string]interface{}
		var regex string
		var ytagstr interface{}
		var ytagnum int64

		ytagstr, ok = syncconfig["ytagbase"]
		if ok {
			ytagnum, err = strconv.ParseInt(ytagstr.(string), 0, 32)
			if err != nil {
				fmt.Println("ytagbase must be an integer")
			} else {
				ytagbase = int(ytagnum)
			}
		} else {
			fmt.Println("Configuration file does not specify ytagbase. Defaulting to 0.")
		}
		regex, ok = syncconfig["name_regex"].(string)
		if !ok {
			fmt.Println("Configuration file does not specify name_regex. Defaulting to the empty string.")
			regex = ""
		}

	uPMULoop:
		for ip, temp = range config {
			uuids = make([]string, 0, len(upmuparser.STREAMS))
			upmuMap = temp.(map[string]interface{})
			temp, ok = upmuMap["%serial_number"]
			if !ok {
				fmt.Printf("Serial number of uPMU with IP Address %v is not specified. Skipping uPMU...\n", ip)
				continue
			}
			serial = temp.(string)
			temp, ok = upmuMap["%alias"]
			if ok {
				alias = temp.(string)
			} else {
				alias = serial
			}
			for i = 0; i < len(upmuparser.STREAMS); i++ {
				temp, ok = upmuMap[upmuparser.STREAMS[i]]
				if !ok {
					break
				}
				streamMap = temp.(map[string]interface{})
				temp, ok = streamMap["uuid"]
				if !ok {
					fmt.Printf("UUID is missing for stream %v of uPMU %v. Skipping uPMU...\n", upmuparser.STREAMS[i], alias)
					continue uPMULoop
				}
				uustr := temp.(string)
				uuids = append(uuids, uustr)

				/* Create the stream if not already created. */
				var pathint interface{}
				if pathint, ok = streamMap["Path"]; ok {
					path := pathint.(string)
					pparts := strings.Split(path, "/")
					collname := strings.Join(pparts[:len(pparts)-1], "/")
					streamname := pparts[len(pparts)-1]
					uu := uuid.Parse(uustr)
					s := btrdbconn.StreamFromUUID(uu)
					ex, err := s.Exists(context.Background())
					if err != nil {
						fmt.Printf("Warning: could not check if stream exists: %v", err)
					} else if !ex {
						fmt.Printf("Creating stream %s (%s)\n", uu.String(), path)
						s, err = btrdbconn.Create(context.Background(), uu, collname, map[string]string{"name": streamname}, nil)
						if err != nil {
							fmt.Printf("Warning: could not create stream: %v", err)
						}
					} else {
						fmt.Printf("Stream %s (%s) already exists\n", uu.String(), path)
					}
				}
			}
			fmt.Printf("Starting process loop of uPMU %v\n", alias)
			go startProcessLoop(ctx, serial, alias, uuids, &alive, complete, regex)
			num_uPMUs++
		}

		for i = 0; i < num_uPMUs; i++ {
			<-complete // block the main thread until all the goroutines say they're done
		}

		if num_uPMUs == 0 {
			fmt.Println("WARNING: No uPMUs found. Sleeping forever...")
			for alive {
				time.Sleep(time.Second)
			}
		}
	}
}

func startProcessLoop(ctx context.Context, serial_number string, alias string, uuid_strings []string, alivePtr *bool, finishSig chan bool, nameRegex string) {
	var uuids = make([]uuid.UUID, len(uuid_strings))

	var i int

	for i = 0; i < len(uuids); i++ {
		uuids[i] = uuid.Parse(uuid_strings[i])
	}
	mgo_addr := os.Getenv("MONGO_ADDR")
	if mgo_addr == "" {
		mgo_addr = "localhost:27017"
	}

	session, err := mgo.Dial(mgo_addr)
	if err != nil {
		fmt.Printf("Error connecting to Mongo database of received files for %v: %v\n", alias, err)
		finishSig <- false
		return
	}
	session.SetSyncTimeout(0)
	session.SetSocketTimeout(24 * time.Hour)
	c := session.DB("upmu_database").C("received_files")

	fmt.Println("Verifying that database indices exist...")
	err = c.EnsureIndex(mgo.Index{
		Key: []string{"serial_number", "ytag", "name"},
	})

	if err == nil {
		err = c.EnsureIndex(mgo.Index{
			Key: []string{"serial_number", "name"},
		})
	}

	if err != nil {
		fmt.Printf("Could not build indices on Mongo database: %v\nTerminating program...", err)
		*alivePtr = false
		finishSig <- false
		return
	}

	process_loop(ctx, alivePtr, c, serial_number, alias, uuids, btrdbconn, nameRegex)

	session.Close()
	finishSig <- true
}

func insert_stream(ctx context.Context, uu uuid.UUID, output *upmuparser.Sync_Output, getValue upmuparser.InsertGetter, startTime int64, bc *btrdb.BTrDB, feedback chan int) {
	var sampleRate float32 = output.SampleRate()
	var numPoints int = int((1000.0 / sampleRate) + 0.5)
	var timeDelta float64 = float64(sampleRate) * 1000000 // convert to nanoseconds

	stream := bc.StreamFromUUID(uu)

	points := make([]btrdb.RawPoint, numPoints)
	for i := 0; i != len(points); i++ {
		points[i].Time = startTime + int64((float64(i)*timeDelta)+0.5)
		points[i].Value = getValue(i, output)
	}

	err := stream.Insert(ctx, points)
	if err == nil {
		feedback <- 0
	} else {
		fmt.Printf("Error inserting data: %v\n", err)
		feedback <- 1
	}
}

func process(ctx context.Context, coll *mgo.Collection, query map[string]interface{}, sernum string, alias string, uuids []uuid.UUID, bc *btrdb.BTrDB, alive *bool) bool {
	var documents *mgo.Iter = coll.Find(query).Sort("name").Iter()

	var result map[string]interface{} = make(map[string]interface{})

	var continueIteration bool = documents.Next(&result)

	var rawdata []uint8
	var parsed []*upmuparser.Sync_Output
	var synco *upmuparser.Sync_Output
	var timeArr [6]int32
	var i int
	var j int
	var numsent int
	var timestamp int64
	var feedback chan int
	var success bool
	var err error
	var igs []upmuparser.InsertGetter
	var ig upmuparser.InsertGetter

	var documentsFound bool = false

	for continueIteration {
		documentsFound = true

		success = true
		rawdata = result["data"].([]uint8)
		parsed, err = upmuparser.ParseSyncOutArray(rawdata)
		feedback = make(chan int)
		numsent = 0
		for i = 0; i < len(parsed); i++ {
			synco = parsed[i]
			if synco == nil {
				var file *os.File
				fmt.Printf("Could not parse set at index %d in file %s from uPMU %s (serial=%s). Reason: %v\n", i, result["name"].(string), alias, sernum, err)
				if err == io.ErrUnexpectedEOF {
					fmt.Println("Warning: skipping partially written/corrupt set...")
					continue
				} else {
					fmt.Println("Dumping bad file into error.dat...")
					file, err = os.OpenFile("error.dat", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
					if err == nil {
						_, err = file.Write(rawdata)
					}
					if err == nil {
						err = file.Close()
					}
					if err == nil {
						fmt.Println("Finished writing file.")
					} else {
						fmt.Printf("Could not dump bad file: %v\n", err)
					}
					os.Exit(1)
				}
			}
			timeArr = synco.Times()
			if timeArr[0] < 2010 || timeArr[0] > 2020 {
				// if the year is outside of this range things must have gotten corrupted somehow
				fmt.Printf("Rejecting bad date record for %v: year is %v\n", alias, timeArr[0])
				continue
			}
			timestamp = time.Date(int(timeArr[0]), time.Month(timeArr[1]), int(timeArr[2]), int(timeArr[3]), int(timeArr[4]), int(timeArr[5]), 0, time.UTC).UnixNano()
			igs = synco.GetInsertGetters()
			for j, ig = range igs {
				if j >= len(uuids) {
					fmt.Printf("Warning: data for a stream includes stream %s, but no UUID was provided for that stream. Dropping data for that stream...\n", upmuparser.STREAMS[j])
					continue
				}
				go insert_stream(ctx, uuids[j], synco, ig, timestamp, bc, feedback)
				numsent++
			}
		}
		for j = 0; j < numsent; j++ {
			if <-feedback == 1 {
				fmt.Printf("Warning: data for a stream could not be sent for uPMU %v (serial=%v)\n", alias, sernum)
				success = false
			}
		}
		fmt.Printf("Finished sending %v for uPMU %v (serial=%v)\n", result["name"], alias, sernum)

		if success {
			fmt.Printf("Updating ytag for %v for uPMU %v (serial=%v)\n", result["name"], alias, sernum)
			err = coll.Update(map[string]interface{}{
				"_id": result["_id"],
			}, map[string]interface{}{
				"$set": map[string]interface{}{
					"ytag": ytagbase,
				},
			})

			if err == nil {
				fmt.Printf("Successfully updated ytag for %v for uPMU %v (serial=%v)\n", result["name"], alias, sernum)
			} else {
				fmt.Printf("Could not update ytag for a document for uPMU %v: %v\n", alias, err)
			}
		} else {
			fmt.Println("Document insert fails. Terminating program...")
			*alive = false
		}
		if !(*alive) {
			break
		}
		continueIteration = documents.Next(&result)
		if !continueIteration && documents.Timeout() {
			continueIteration = documents.Next(&result)
		}
	}

	err = documents.Err()
	if err != nil {
		fmt.Printf("Could not iterate through documents for uPMU %v: %v\nTerminating program...", alias, err)
		*alive = false
	}

	return documentsFound
}

func process_loop(ctx context.Context, keepalive *bool, coll *mgo.Collection, sernum string, alias string, uuids []uuid.UUID, bc *btrdb.BTrDB, nameRegex string) {
	query := map[string]interface{}{
		"serial_number": sernum,
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
	if nameRegex != "" {
		query["name"] = map[string]interface{}{
			"$regex": nameRegex,
		}
	}
	var i int
	for *keepalive {
		fmt.Printf("looping %v\n", alias)
		if process(ctx, coll, query, sernum, alias, uuids, bc, keepalive) {
			fmt.Printf("sleeping %v\n", alias)
			time.Sleep(time.Second)
		} else {
			fmt.Printf("No documents found for %v. Waiting 100 seconds...\n", alias)
			for i = 0; i < 100 && *keepalive; i++ {
				time.Sleep(time.Second)
			}
		}
	}
	fmt.Printf("Terminated process loop for %v\n", alias)
}
