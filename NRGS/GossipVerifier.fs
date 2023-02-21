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
        toVerifyMsgHandler: BufferBlock<IRoutingMsg * array<byte>>,
        verifiedMsgHandler: BufferBlock<IRoutingMsg * array<byte>>
    ) =
    member __.Start() =
        async {
            let! cancelToken = Async.CancellationToken
            cancelToken.ThrowIfCancellationRequested()

            let network = UtxoCoin.Account.GetNetwork Currency.BTC

            while true do
                let! msg, bytes =
                    toVerifyMsgHandler.ReceiveAsync cancelToken
                    |> Async.AwaitTask

                match msg with
                | :? ChannelAnnouncementMsg as channelAnn ->
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
                        verifiedMsgHandler.Post(msg, bytes) |> ignore
                    | Some _ ->
                        Console.WriteLine
                            "Channel announcement key didn't match on-chain script"
                    | None ->
                        Console.WriteLine
                            "Output index out of bounds in transaction"
                | :? ChannelUpdateMsg as _updateMsg ->
                    //TODO: verify msg signature
                    verifiedMsgHandler.Post(msg, bytes) |> ignore
                | _ -> ()

            ()
        }
