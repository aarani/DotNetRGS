namespace NRGS

open System
open System.Threading
open System.Threading.Tasks.Dataflow

open DotNetLightning.Utils

open GWallet.Backend
open GWallet.Backend.FSharpUtil.AsyncExtensions
open GWallet.Backend.UtxoCoin.Lightning

module Program =

    [<EntryPoint>]
    let main argv =
        async {
            let graph = NetworkGraph()

            let blockOption = DataflowBlockOptions()

            let toVerify = BufferBlock<Message> blockOption
            let toHandle = BufferBlock<Message> blockOption

            let snapshotStartSource = new CancellationTokenSource()

            let syncer =
                GossipSyncer(
                    NodeIdentifier.TcpEndPoint(
                        NodeEndPoint.Parse
                            Currency.BTC
                            "035e4ff418fc8b5554c5d9eea66396c227bd429a3251c8cbc711002ba215bfc226@170.75.163.209:9735"
                    ),
                    toVerify
                )

            let verifier1 = GossipVerifier(toVerify, toHandle, graph)

            let persistence = GossipPersistence(toHandle, graph, snapshotStartSource)

            let snapshotter = GossipSnapshotter snapshotStartSource.Token

            Console.WriteLine(
                sprintf "NRGS[%s]: started" (DateTime.UtcNow.ToString())
            )

            do!
                MixedParallel4
                    (syncer.Start())
                    (verifier1.Start())
                    (persistence.Start())
                    (snapshotter.Start())
                |> Async.Ignore
        }
        |> Async.RunSynchronously

        0
