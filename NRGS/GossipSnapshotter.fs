namespace NRGS

open System
open System.Threading

type GossipSnapshotter(startToken: CancellationToken) =
    member __.Start() =
        async {
            do! startToken.WaitHandle |> Async.AwaitWaitHandle |> Async.Ignore

            let rec snapshot() =
                async {
                    //TODO: create snapshots


                    let nextSnapshot = DateTime.UtcNow.Date.AddDays 1.

                    // sleep until next day
                    // FIXME: is it possible that this cause problem when we're exteremely close to the end of day? (negetive timespan and etc)
                    do! Async.Sleep(nextSnapshot.Subtract DateTime.UtcNow)
                    return! snapshot()
                }

            do! snapshot()
        }
