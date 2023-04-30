namespace NRGS

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
        verifiedMsgHandler: BufferBlock<Message>
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
                | RoutingMsg(:? ChannelAnnouncementMsg as channelAnn, _bytes) ->
                    //TODO: verify msg signatures
                    let blockHeight =
                        channelAnn.Contents.ShortChannelId.BlockHeight.Value

                    let blockIndex =
                        channelAnn.Contents.ShortChannelId.BlockIndex.Value

                    let txOutIndex =
                        channelAnn.Contents.ShortChannelId.TxOutIndex.Value


                    Console.WriteLine(
                        sprintf
                            "Looking to verify #%i,#%i,#%i"
                            blockHeight
                            blockIndex
                            txOutIndex
                    )

#if !DEBUG
                    let! txId =
                        Server.Query
                            Currency.BTC
                            (QuerySettings.Default ServerSelectionMode.Fast)
                            (ElectrumClient.GetBlockchainTransactionIdFromPos
                                blockHeight
                                blockIndex)
                            None

                    let! transaction =
                        Server.Query
                            Currency.BTC
                            (QuerySettings.Default ServerSelectionMode.Fast)
                            (ElectrumClient.GetBlockchainTransaction txId)
                            None

                    let transaction = Transaction.Parse(transaction, network)

                    let redeem =
                        Scripts.funding
                            (FundingPubKey channelAnn.Contents.BitcoinKey1.Value)
                            (FundingPubKey channelAnn.Contents.BitcoinKey2.Value)

                    let outputOpt =
                        transaction.Outputs |> Seq.tryItem(int txOutIndex)

                    match outputOpt with
                    | Some output when
                        output.IsTo(redeem.WitHash :> IDestination)
                        ->
                        do!
                            verifiedMsgHandler.SendAsync msg
                            |> Async.AwaitTask
                            |> Async.Ignore
                    | Some _ ->
                        Console.WriteLine
                            "Channel announcement key didn't match on-chain script"
                    | None ->
                        Console.WriteLine
                            "Output index out of bounds in transaction"
#else
                    do!
                        verifiedMsgHandler.SendAsync msg
                        |> Async.AwaitTask
                        |> Async.Ignore
#endif
                | RoutingMsg(:? ChannelUpdateMsg as _updateMsg, _bytes) ->
                    //TODO: verify msg signature
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
