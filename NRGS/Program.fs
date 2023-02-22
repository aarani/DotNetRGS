namespace NRGS

open System.Threading.Tasks.Dataflow

open DotNetLightning.Serialization.Msgs
open DotNetLightning.Utils

open GWallet.Backend
open GWallet.Backend.FSharpUtil.AsyncExtensions
open GWallet.Backend.UtxoCoin.Lightning

module Program =

    [<EntryPoint>]
    let main argv =
        async {
            let blockOption = DataflowBlockOptions()

            let toVerify = BufferBlock<Message> blockOption
            let toHandle = BufferBlock<Message> blockOption

            let syncer =
                GossipSyncer(
                    NodeIdentifier.TcpEndPoint(
                        NodeEndPoint.Parse
                            Currency.BTC
                            "035e4ff418fc8b5554c5d9eea66396c227bd429a3251c8cbc711002ba215bfc226@170.75.163.209:9735"
                    ),
                    toVerify
                )

            let verifier1 = GossipVerifier(toVerify, toHandle)
            let verifier2 = GossipVerifier(toVerify, toHandle)

            let persistence = GossipPersistence(toHandle)

            do!
                MixedParallel4
                    (syncer.Start())
                    (verifier1.Start())
                    (verifier2.Start())
                    (persistence.Start())
                |> Async.Ignore
        }
        |> Async.RunSynchronously

        0
