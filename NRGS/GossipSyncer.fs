namespace NRGS

open System
open System.Threading.Tasks.Dataflow

open DotNetLightning.Serialization.Msgs
open DotNetLightning.Utils

open GWallet.Backend
open GWallet.Backend.FSharpUtil.ReflectionlessPrint
open GWallet.Backend.UtxoCoin.Lightning

open NBitcoin

open ResultUtils.Portability

exception RoutingQueryException of string

type internal GossipSyncer
    (
        peer: NodeIdentifier,
        toVerifyMsgHandler: BufferBlock<IRoutingMsg * array<byte>>
    ) =
    member __.Start() =
        async {
            let! cancelToken = Async.CancellationToken
            cancelToken.ThrowIfCancellationRequested()

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
                let toUnixTimestamp datetime =
                    (datetime - DateTime(1970, 1, 1)).TotalSeconds |> uint32

                toUnixTimestamp(DateTime.Now - TimeSpan.FromDays(14.0))

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
                        toVerifyMsgHandler.Post(msg, bytes) |> ignore

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
