namespace DotNetRGS

open System
open System.Threading.Tasks.Dataflow

open DotNetLightning.Serialization.Msgs
open DotNetLightning.Utils
open ResultUtils.Portability
open GWallet.Backend
open GWallet.Backend.FSharpUtil.ReflectionlessPrint
open GWallet.Backend.UtxoCoin.Lightning
open NBitcoin

open DotNetRGS.Utils

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

    let mutable finishedInitialSync: bool = false

    member private __.Download() =
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

            let initialNode =
                match initialNode with
                | Ok node -> node
                | Error e -> failwith "connecting to peer failed"

            let chainHash = Network.Main.GenesisHash

            let recvMsg msgStream =
                async {
                    let! recvBytesRes = msgStream.TransportStream.RecvBytes()

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
                                        TransportStream = transportStream
                                    },
                                    msg,
                                    bytes
                                )
                }

            let doInitialSync(node: PeerNode) : Async<PeerNode> =
                async {
                    let firstBlocknum = 0u
                    let numberOfBlocks = 0xffffffffu

                    let queryMsg =
                        {
                            QueryChannelRangeMsg.ChainHash = chainHash
                            FirstBlockNum = BlockHeight firstBlocknum
                            NumberOfBlocks = numberOfBlocks
                            TLVs = [||]
                        }

                    // Step 1: send query_channel_range, read all replies and collect short channel ids from them
                    let! node = node.SendMsg queryMsg

                    let shortChannelIds = ResizeArray<ShortChannelId>()

                    let rec queryShortChannelIds
                        (node: PeerNode)
                        : Async<PeerNode> =
                        async {
                            let! response = recvMsg node.MsgStream

                            match response with
                            | Error e ->
                                return
                                    raise <| RoutingQueryException(e.ToString())
                            | Ok
                                (
                                    newState,
                                    (
                                        :? ReplyChannelRangeMsg as replyChannelRange
                                    ),
                                    _bytes
                                ) ->
                                let node =
                                    { node with
                                        MsgStream = newState
                                    }

                                shortChannelIds.AddRange
                                    replyChannelRange.ShortIds

                                if replyChannelRange.Complete then
                                    return node
                                else
                                    return! queryShortChannelIds node
                            | Ok(newState, msg, _bytes) ->
                                // ignore all other messages
                                let logMsg =
                                    SPrintF1
                                        "Received unexpected message while processing reply_channel_range messages:\n %A"
                                        msg

                                Infrastructure.LogDebug logMsg

                                return!
                                    queryShortChannelIds
                                        { node with
                                            MsgStream = newState
                                        }
                        }

                    let! node = queryShortChannelIds node

                    let batchSize = 1000

                    let batches =
                        shortChannelIds
                        |> Seq.chunkBySize batchSize
                        |> Collections.Generic.Queue

                    // Step 2: split shortChannelIds into batches and for each batch:
                    // - send query_short_channel_ids
                    // - receive routing messages and add them to result until we get reply_short_channel_ids_end
                    let rec processMessages(node: PeerNode) : Async<PeerNode> =
                        async {
                            let! response = recvMsg node.MsgStream

                            match response with
                            | Error e ->
                                return
                                    raise <| RoutingQueryException(e.ToString())
                            | Ok(newState, (:? IRoutingMsg as msg), bytes) ->
                                let node =
                                    { node with
                                        MsgStream = newState
                                    }

                                match msg with
                                | :? ReplyShortChannelIdsEndMsg as _channelIdsEnd ->
                                    if batches.Count = 0 then
                                        return node // end processing
                                    else
                                        return! sendNextBatch node
                                | :? ChannelAnnouncementMsg
                                | :? ChannelUpdateMsg ->
                                    do!
                                        toVerifyMsgHandler.SendAsync(
                                            RoutingMsg(msg, bytes),
                                            cancelToken
                                        )
                                        |> Async.AwaitTask
                                        |> Async.Ignore

                                    return! processMessages node
                                | _ ->
                                    //Routing msgs we don't care about
                                    return! processMessages node
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
                            | Ok(newState, msg, _bytes) ->
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

                    and sendNextBatch(node: PeerNode) : Async<PeerNode> =
                        async {
                            let queryShortIdsMsg =
                                {
                                    QueryShortChannelIdsMsg.ChainHash =
                                        chainHash
                                    ShortIdsEncodingType =
                                        EncodingType.SortedPlain
                                    ShortIds = batches.Dequeue()
                                    TLVs = [||]
                                }

                            let! node = node.SendMsg queryShortIdsMsg
                            return! processMessages node
                        }

                    return! sendNextBatch node
                }

            let doGossipTimestampFilterSync(node: PeerNode) =
                async {
                    let firstTimestamp =
                        DateTimeUtils.ToUnixTimestamp DateTime.Now

                    let gossipTimeStampFilter =
                        {
                            GossipTimestampFilterMsg.ChainHash =
                                Network.Main.GenesisHash
                            FirstTimestamp = firstTimestamp
                            TimestampRange = UInt32.MaxValue
                        }

                    let! node = node.SendMsg gossipTimeStampFilter

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
                                            return
                                                Error <| DeserializeMsg msgError
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
                            | Error e ->
                                return failwithf "RecvMsg failed, error = %A" e
                            | Ok(newState, (:? IRoutingMsg as msg), bytes) ->
                                do!
                                    toVerifyMsgHandler.SendAsync(
                                        RoutingMsg(msg, bytes),
                                        cancelToken
                                    )
                                    |> Async.AwaitTask
                                    |> Async.Ignore

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

                    return! processMessages node
                }

            let! node =
                // We don't want to do an entire initial sync everytime we reconnect
                if not finishedInitialSync then
                    doInitialSync initialNode
                else
                    async { return initialNode }

            finishedInitialSync <- true
            Logger.Log "GossipSyncer" "finished a full initial sync"

            do!
                toVerifyMsgHandler.SendAsync FinishedInitialSync
                |> Async.AwaitTask
                |> Async.Ignore

            do! doGossipTimestampFilterSync node |> Async.Ignore
        }

    member self.DownloadWithReconnect() =
        async {
            try
                do! self.Download()
            with
            | ex ->
                Logger.Log
                    "GossipSyncer"
                    (sprintf "Connection failed with the following ex:\n %A" ex)

                return! self.DownloadWithReconnect()
        }

    member self.Start() =
        async {
            let! cancelToken = Async.CancellationToken
            cancelToken.ThrowIfCancellationRequested()

            return! self.DownloadWithReconnect()
        }
