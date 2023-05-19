namespace DotNetRGS.Utils

open System

module DateTimeUtils =
    let private UnixEpoch = DateTime(1970, 1, 1)

    let internal GetTimeSpanSinceEpoch(dt: DateTime) =
        dt - UnixEpoch

    let internal ToUnixTimestamp(dt: DateTime) =
        if dt < UnixEpoch then
            0u
        else
            (GetTimeSpanSinceEpoch dt).TotalSeconds |> uint

    let internal FromUnixTimestamp(num: uint) =
        num |> float |> UnixEpoch.AddSeconds
