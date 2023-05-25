namespace DotNetRGS

open System
open System.IO
open System.Threading
open System.Threading.Tasks.Dataflow

open DotNetLightning.Utils

open GWallet.Backend
open GWallet.Backend.FSharpUtil.AsyncExtensions
open GWallet.Backend.UtxoCoin.Lightning

open Microsoft.Extensions.Configuration

module Program =

    [<EntryPoint>]
    let main argv =
        async {
            let configuration =
                ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json", false, false)
                    .Build()

            let graph = NetworkGraph()

            let blockOption = DataflowBlockOptions()

            let toVerify = BufferBlock<Message> blockOption
            let toHandle = BufferBlock<Message> blockOption

            let snapshotStartSource = new CancellationTokenSource()

            let syncers =
                configuration.GetSection("Peers").Get<string []>()
                |> Seq.map(fun peerInfo ->
                    GossipSyncer(
                        NodeIdentifier.TcpEndPoint(
                            NodeEndPoint.Parse Currency.BTC peerInfo
                        ),
                        toVerify
                    )
                )

            let verifier1 = GossipVerifier(toVerify, toHandle, graph)

            let persistence =
                GossipPersistence(
                    configuration,
                    toHandle,
                    graph,
                    snapshotStartSource
                )

            let snapshotter =
                GossipSnapshotter(
                    configuration,
                    graph,
                    snapshotStartSource.Token
                )

            Logger.Log "DotNetRGS" "started"

            do!
                MixedParallel4
                    (syncers
                     |> Seq.map(fun syncer -> syncer.Start())
                     |> Async.Parallel)
                    (verifier1.Start())
                    (persistence.Start())
                    (snapshotter.Start())
                |> Async.Ignore
        }
        |> Async.RunSynchronously

        0
