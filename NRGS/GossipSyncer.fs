namespace NRGS

open System
open System.IO
open System.Threading.Tasks.Dataflow

open DotNetLightning.Serialization.Msgs
open DotNetLightning.Utils
open ResultUtils.Portability
open GWallet.Backend
open GWallet.Backend.FSharpUtil.AsyncExtensions
open GWallet.Backend.FSharpUtil.ReflectionlessPrint
open GWallet.Backend.UtxoCoin.Lightning
open NBitcoin
open Newtonsoft.Json

open NRGS.Utils

exception RoutingQueryException of string

type SyncState =
    {
        LastSyncTimestamp: DateTime
    }

type internal GossipSyncer
    (
        peer: NodeIdentifier,
        toVerifyMsgHandler: BufferBlock<Message>
    ) =

    let msgCount = ref 0L

    member __.Download() =
        async {
            let! cancelToken = Async.CancellationToken

            let! initialNode =
                let throwawayPrivKey =
                    ExtKey() |> NodeMasterPrivKey.NodeMasterPrivKey

                PeerNode.Connect
                    throwawayPrivKey
                    peer
                    Currency.BTC
                    Money.Zero
                    ConnectionPurpose.Routing

            let firstTimestamp =
                let twoWeeksAgoUnixTime =
                    DateTimeUtils.ToUnixTimestamp(
                        DateTime.Now - TimeSpan.FromDays(14.0)
                    )

                if
                    File.Exists
                        (sprintf "syncState-%s.json" (peer.NodeId.Value.ToHex()))
                then
                    let syncState =
                        File.ReadAllText(
                            sprintf
                                "syncState-%s.json"
                                (peer.NodeId.Value.ToHex())
                        )
                        |> JsonConvert.DeserializeObject<SyncState>

                    let lastSyncUnixTime =
                        DateTimeUtils.ToUnixTimestamp(
                            syncState.LastSyncTimestamp.AddHours(
#if DEBUG
                                0
#else
                                -5
#endif
                            )
                        )

                    Math.Max(lastSyncUnixTime, twoWeeksAgoUnixTime)
                else
                    twoWeeksAgoUnixTime

            let gossipTimeStampFilter =
                {
                    GossipTimestampFilterMsg.ChainHash =
                        Network.Main.GenesisHash
                    FirstTimestamp = firstTimestamp
                    TimestampRange = UInt32.MaxValue
                }

            let! initialNode =
                match initialNode with
                | Ok node -> node.SendMsg gossipTimeStampFilter
                | Error e -> failwith "sendinng gossip timestamp filter failed."

            let rec processMessages(node: PeerNode) : Async<PeerNode> =
                async {
                    cancelToken.ThrowIfCancellationRequested()

                    let recvMsg msgStream =
                        async {
                            let! recvBytesRes =
                                msgStream.TransportStream.RecvBytes()

                            match recvBytesRes with
                            | Error recvBytesError ->
                                return Error <| RecvBytes recvBytesError
                            | Ok(transportStream, bytes) ->
                                match LightningMsg.fromBytes bytes with
                                | Error msgError ->
                                    return Error <| DeserializeMsg msgError
                                | Ok msg ->
                                    return
                                        Ok(
                                            { msgStream with
                                                TransportStream =
                                                    transportStream
                                            },
                                            msg,
                                            bytes
                                        )
                        }

                    let! response = recvMsg node.MsgStream

                    match response with
                    | Error e -> return failwithf "RecvMsg failed, error = %A" e
                    | Ok(newState, (:? IRoutingMsg as msg), bytes) ->
                        System.Threading.Interlocked.Increment msgCount
                        |> ignore<int64>

                        toVerifyMsgHandler.SendAsync(
                            RoutingMsg(msg, bytes),
                            cancelToken
                        )
                        |> ignore

                        return!
                            processMessages
                                { node with
                                    MsgStream = newState
                                }
                    | Ok(newState, (:? PingMsg as pingMsg), _) ->
                        let! msgStreamAfterPongSent =
                            newState.SendMsg
                                {
                                    PongMsg.BytesLen = pingMsg.PongLen
                                }

                        return!
                            processMessages
                                { node with
                                    MsgStream = msgStreamAfterPongSent
                                }
                    | Ok(newState, msg, _) ->
                        // ignore all other messages
                        let logMsg =
                            SPrintF1
                                "Received unexpected message while processing routing messages:\n %A"
                                msg

                        Infrastructure.LogDebug logMsg

                        return!
                            processMessages
                                { node with
                                    MsgStream = newState
                                }
                }

            do! processMessages initialNode |> Async.Ignore
        }

    member __.LookForInitialSyncFinish() =
        async {
            let mutable previousCounter = 0L
            let mutable i = 0UL

            let rec lookForFinishedSync() =
                async {
                    i <- i + 1UL
                    do! Async.Sleep(TimeSpan.FromSeconds 5.)

                    let newMsgCount = msgCount.Value
                    let delta = newMsgCount - previousCounter

                    Console.WriteLine(
                        sprintf
                            "initial sync gossip count (iteration %i): %i (delta: %i)"
                            i
                            newMsgCount
                            delta
                    )

                    if i > 2UL && delta < 50 && previousCounter <> 0 then
                        Console.WriteLine("Initial sync finished")

                        let syncState =
                            {
                                LastSyncTimestamp = DateTime.UtcNow
                            }
                            |> JsonConvert.SerializeObject

                        File.WriteAllText(
                            sprintf
                                "syncState-%s.json"
                                (peer.NodeId.Value.ToHex()),
                            syncState
                        )

                        return!
                            toVerifyMsgHandler.SendAsync(FinishedInitialSync)
                            |> Async.AwaitTask
                            |> Async.Ignore
                    else
                        previousCounter <- newMsgCount
                        do! lookForFinishedSync()
                }

            do! lookForFinishedSync()
        }

    member self.Start() =
        async {
            let! cancelToken = Async.CancellationToken
            cancelToken.ThrowIfCancellationRequested()

            do!
                MixedParallel2
                    (self.Download())
                    (self.LookForInitialSyncFinish())
                |> Async.Ignore
        }
