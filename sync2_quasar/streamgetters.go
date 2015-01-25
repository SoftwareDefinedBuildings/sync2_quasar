package main

import "github.com/SoftwareDefinedBuildings/sync2_quasar/parser"

/* These are functions to use for getting values for streams. It's not concise,
   but it's needed since the data is parsed to structs, which as far as I know
   can't be subscripted with strings. */

func GetL1Mag(index int, obj *parser.Sync_Output) float64 {
    return float64(obj.Sync_Data.L1MagAng[index].Mag)
}

func GetL1Ang(index int, obj *parser.Sync_Output) float64 {
    return float64(obj.Sync_Data.L1MagAng[index].Angle)
}

func GetL2Mag(index int, obj *parser.Sync_Output) float64 {
    return float64(obj.Sync_Data.L2MagAng[index].Mag)
}

func GetL2Ang(index int, obj *parser.Sync_Output) float64 {
    return float64(obj.Sync_Data.L2MagAng[index].Angle)
}

func GetL3Mag(index int, obj *parser.Sync_Output) float64 {
    return float64(obj.Sync_Data.L3MagAng[index].Mag)
}

func GetL3Ang(index int, obj *parser.Sync_Output) float64 {
    return float64(obj.Sync_Data.L3MagAng[index].Angle)
}

func GetC1Mag(index int, obj *parser.Sync_Output) float64 {
    return float64(obj.Sync_Data.C1MagAng[index].Mag)
}

func GetC1Ang(index int, obj *parser.Sync_Output) float64 {
    return float64(obj.Sync_Data.C1MagAng[index].Angle)
}

func GetC2Mag(index int, obj *parser.Sync_Output) float64 {
    return float64(obj.Sync_Data.C2MagAng[index].Mag)
}

func GetC2Ang(index int, obj *parser.Sync_Output) float64 {
    return float64(obj.Sync_Data.C2MagAng[index].Angle)
}

func GetC3Mag(index int, obj *parser.Sync_Output) float64 {
    return float64(obj.Sync_Data.C3MagAng[index].Mag)
}

func GetC3Ang(index int, obj *parser.Sync_Output) float64 {
    return float64(obj.Sync_Data.C3MagAng[index].Angle)
}

func GetLockState(index int, obj *parser.Sync_Output) float64 {
    return float64(obj.Sync_Data.Lockstate[index])
}

var insertGetters [13]func(int, *parser.Sync_Output) float64 = [13]func(int, *parser.Sync_Output) float64{GetL1Mag, GetL1Ang, GetL2Mag, GetL2Ang, GetL3Mag, GetL3Ang, GetC1Mag, GetC1Ang, GetC2Mag, GetC2Ang, GetC3Mag, GetC3Ang, GetLockState}
var STREAMS [13]string = [13]string{"L1MAG", "L1ANG", "L2MAG", "L2ANG", "L3MAG", "L3ANG","C1MAG", "C1ANG", "C2MAG", "C2ANG", "C3MAG", "C3ANG", "LSTATE"}
