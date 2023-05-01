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
            let blockOption = DataflowBlockOptions()

            let toVerify = BufferBlock<Message> blockOption
            let toHandle = BufferBlock<Message> blockOption

            let snapshotStartSource = new CancellationTokenSource()

            let syncer =
                GossipSyncer(
                    NodeIdentifier.TcpEndPoint(
                        NodeEndPoint.Parse
                            Currency.BTC
                            "02f1a8c87607f415c8f22c00593002775941dea48869ce23096af27b0cfdcc0b69@52.13.118.208:9735"
                    ),
                    toVerify
                )

            let verifier1 = GossipVerifier(toVerify, toHandle)

            let persistence = GossipPersistence(toHandle, snapshotStartSource)

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
