namespace NRGS.Utils

open System

module DateTimeUtils =
    let private UnixEpoch = DateTime(1970, 1, 1)

    let internal GetTimeSpanSinceEpoch(dt: DateTime) =
        dt - UnixEpoch

    let internal ToUnixTimestamp(dt: DateTime) =
        (GetTimeSpanSinceEpoch dt).TotalSeconds |> uint

    let internal FromUnixTimestamp(num: uint) =
        num |> float |> UnixEpoch.AddSeconds
