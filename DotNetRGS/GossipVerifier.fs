namespace DotNetRGS

open System
open System.Threading.Tasks.Dataflow

open DotNetLightning.Serialization.Msgs
open DotNetLightning.Transactions
open DotNetLightning.Utils

open GWallet.Backend.UtxoCoin
open GWallet.Backend

open NBitcoin

type internal GossipVerifier
    (
        toVerifyMsgHandler: BufferBlock<Message>,
        verifiedMsgHandler: BufferBlock<Message>,
        graph: NetworkGraph
    ) =
    member __.Start() =
        async {
            let! cancelToken = Async.CancellationToken
            cancelToken.ThrowIfCancellationRequested()

            let network = UtxoCoin.Account.GetNetwork Currency.BTC

            while true do
                let! msg =
                    toVerifyMsgHandler.ReceiveAsync cancelToken
                    |> Async.AwaitTask

                match msg with
                | RoutingMsg(:? ChannelAnnouncementMsg as channelAnn, bytes) ->
                    let saveChannelAnn(capacityOpt: Option<Money>) =
                        VerifiedChannelAnnouncement(
                            channelAnn,
                            capacityOpt,
                            bytes
                        )
                        |> verifiedMsgHandler.SendAsync
                        |> Async.AwaitTask
                        |> Async.Ignore

                    if graph.ValidateChannelAnnouncement channelAnn.Contents then
                        let hasValidSigs =
                            let hash =
                                NBitcoin.Crypto.Hashes.DoubleSHA256(
                                    channelAnn.Contents.ToBytes()
                                )

                            let verify
                                (key: PubKey)
                                (signature: LNECDSASignature)
                                =
                                key.Verify(hash, signature.Value)

                            verify
                                channelAnn.Contents.BitcoinKey1.Value
                                channelAnn.BitcoinSignature1
                            && verify
                                channelAnn.Contents.BitcoinKey2.Value
                                channelAnn.BitcoinSignature2
                            && verify
                                channelAnn.Contents.NodeId1.Value
                                channelAnn.NodeSignature1
                            && verify
                                channelAnn.Contents.NodeId2.Value
                                channelAnn.NodeSignature2

                        if hasValidSigs then
                            let blockHeight =
                                channelAnn.Contents.ShortChannelId.BlockHeight.Value

                            let blockIndex =
                                channelAnn.Contents.ShortChannelId.BlockIndex.Value

                            let txOutIndex =
                                channelAnn.Contents.ShortChannelId.TxOutIndex.Value


                            Logger.Log
                                "GossipVerifier"
                                (sprintf
                                    "Looking to verify #%i,#%i,#%i"
                                    blockHeight
                                    blockIndex
                                    txOutIndex)

#if !DEBUG
                            let! txId =
                                Server.Query
                                    Currency.BTC
                                    (QuerySettings.Default
                                        ServerSelectionMode.Fast)
                                    (ElectrumClient.GetBlockchainTransactionIdFromPos
                                        blockHeight
                                        blockIndex)
                                    None

                            let! transaction =
                                Server.Query
                                    Currency.BTC
                                    (QuerySettings.Default
                                        ServerSelectionMode.Fast)
                                    (ElectrumClient.GetBlockchainTransaction
                                        txId)
                                    None

                            let transaction =
                                Transaction.Parse(transaction, network)

                            let redeem =
                                Scripts.funding
                                    (FundingPubKey
                                        channelAnn.Contents.BitcoinKey1.Value)
                                    (FundingPubKey
                                        channelAnn.Contents.BitcoinKey2.Value)

                            let outputOpt =
                                transaction.Outputs
                                |> Seq.tryItem(int txOutIndex)

                            match outputOpt with
                            | Some output when
                                output.IsTo(redeem.WitHash :> IDestination)
                                ->
                                do! saveChannelAnn(Some output.Value)
                            | Some _ ->
                                Logger.Log
                                    "GossipVerifier"
                                    "Channel announcement key didn't match on-chain script"
                            | None ->
                                Logger.Log
                                    "GossipVerifier"
                                    "Output index out of bounds in transaction"
#else
                            do! saveChannelAnn None
#endif
                    else
                        Logger.Log
                            "GossipVerifier"
                            "Received channel ann with invalid signature"
                | RoutingMsg(:? ChannelUpdateMsg as updateMsg, _bytes) ->
                    do!
                        verifiedMsgHandler.SendAsync msg
                        |> Async.AwaitTask
                        |> Async.Ignore
                | _ ->
                    // I don't know this, passing it along
                    do!
                        verifiedMsgHandler.SendAsync msg
                        |> Async.AwaitTask
                        |> Async.Ignore

            ()
        }
