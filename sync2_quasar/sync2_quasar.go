package main

import (
    "fmt"
    "gopkg.in/mgo.v2"
    "github.com/SoftwareDefinedBuildings/sync2_quasar/parser"
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
