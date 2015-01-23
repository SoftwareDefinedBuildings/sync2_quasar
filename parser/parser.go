package parser

import (
    "encoding/binary"
    "fmt"
    "io"
    )

type Sync_Point struct {
    Angle float32
    Mag float32
} // 8 bytes

type Sync_Output_Msgq struct {
    SampleRate float32
    Times [6]int32
    Lockstate [120]int32
    L1MagAng [120]Sync_Point
    L2MagAng [120]Sync_Point
    L3MagAng [120]Sync_Point
    C1MagAng [120]Sync_Point
    C2MagAng [120]Sync_Point
    C3MagAng [120]Sync_Point
} // 6268 bytes

type Sync_Pll_Stats_Msgq struct {
    Ppl_State uint32
    Pps_Prd uint32
    Curr_Err int32
    Center_Frq_Offset int32
} // 16 bytes

type Sync_Gps_Stats struct {
    Alt float32
    Lat float32
    Hdop float32
    Lon float32
    Satellites float32
    State float32
    HasFix float32
} // 28 bytes

type Sync_Output struct {
    Sync_Data Sync_Output_Msgq
    Pll_Stats Sync_Pll_Stats_Msgq
    Gps_Stats Sync_Gps_Stats
} // 6312 bytes

const SYNC_OUTPUT_SIZE int = 6312

type decoder struct {
    index int
    data []uint8
}

func (d *decoder) Read(b []byte) (n int, err error) {
    var i int
    n = len(b)
    var outOfSpace bool = false
    if len(d.data) - d.index < n {
        n = len(d.data) - d.index
        outOfSpace = true
    }
    for i = 0; i < n; i++ {
        b[i] = d.data[d.index + i]
    }
    d.index = d.index + n
    if outOfSpace {
        err = io.EOF
    }
    return
}

func parse_sync_output(d *decoder) *Sync_Output {
    var output *Sync_Output = &Sync_Output{}
    err := binary.Read(d, binary.LittleEndian, output)
    if err != nil {
        fmt.Printf("Error parsing sync_output: %v\n", err)
        return nil
    }
    return output
}

func ParseSyncOutArray(data []byte) []*Sync_Output {
    var dataLen int = len(data)
    if dataLen % SYNC_OUTPUT_SIZE != 0 {
        fmt.Printf("WARNING: a whole number of sync_outputs is not present in the data. Data size is %v bytes, which is not a multiple of %v bytes. File may be corrupted.\n", dataLen, SYNC_OUTPUT_SIZE)
    }
    var numSyncOutputs int = dataLen / SYNC_OUTPUT_SIZE
    
    var dec *decoder = &decoder{index: 0, data: data}
    var outputs = make([]*Sync_Output, numSyncOutputs, numSyncOutputs)
    
    for i := 0; i < numSyncOutputs; i++ {
        outputs[i] = parse_sync_output(dec)
    }
    
    return outputs
}
