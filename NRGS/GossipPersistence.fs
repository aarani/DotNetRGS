namespace NRGS

open System
open System.Threading.Tasks.Dataflow

open DotNetLightning.Serialization.Msgs
open DotNetLightning.Utils

open Npgsql
open System.Threading

type internal GossipPersistence
    (
        verifiedMsgHandler: BufferBlock<Message>,
        notifySnapshotter: CancellationTokenSource
    ) =

    member self.Start() =
        async {
            let! cancelToken = Async.CancellationToken
            cancelToken.ThrowIfCancellationRequested()

            let connectionString =
                "Host=127.0.0.1;Username=postgres;Password=f50d47dc6afe40918afa2a935637ec1e;Database=nrgs"

            use dataSource = NpgsqlDataSource.Create(connectionString)

            while true do
                let! msg =
                    verifiedMsgHandler.ReceiveAsync cancelToken
                    |> Async.AwaitTask

                match msg with
                | RoutingMsg(:? ChannelAnnouncementMsg as channelAnn, bytes) ->
                    let sqlCommand =
                        dataSource.CreateCommand(
                            "INSERT INTO channel_announcements (short_channel_id, announcement_signed) VALUES ($1, $2) ON CONFLICT (short_channel_id) DO NOTHING"
                        )

                    sqlCommand.Parameters.AddWithValue(
                        channelAnn.Contents.ShortChannelId.ToUInt64() |> int64
                    )
                    |> ignore<NpgsqlParameter>

                    sqlCommand.Parameters.AddWithValue bytes
                    |> ignore<NpgsqlParameter>

                    do!
                        sqlCommand.ExecuteNonQueryAsync()
                        |> Async.AwaitTask
                        |> Async.Ignore

                    Console.WriteLine(
                        sprintf
                            "Added channel #%i"
                            (channelAnn.Contents.ShortChannelId.ToUInt64())
                    )

                | RoutingMsg(:? ChannelUpdateMsg as updateMsg, bytes) ->
                    let scid =
                        updateMsg.Contents.ShortChannelId.ToUInt64() |> int64

                    let timestamp = updateMsg.Contents.Timestamp |> int64

                    let direction =
                        (updateMsg.Contents.ChannelFlags &&& 1uy) = 1uy

                    let disable =
                        (updateMsg.Contents.ChannelFlags &&& 2uy) > 0uy

                    let cltvExpiryDelta =
                        updateMsg.Contents.CLTVExpiryDelta.Value |> int

                    let htlcMinimumMsat =
                        updateMsg.Contents.HTLCMinimumMSat.MilliSatoshi |> int64

                    let feeBaseMsat =
                        updateMsg.Contents.FeeBaseMSat.MilliSatoshi |> int

                    let feeProportionalMillionths =
                        updateMsg.Contents.FeeProportionalMillionths |> int

                    let htlcMaximumMsat =
                        updateMsg.Contents.HTLCMaximumMSat.Value.MilliSatoshi |> int64

                    let sqlCommand =
                        dataSource.CreateCommand(
                            """
INSERT INTO channel_updates (
	short_channel_id,
	timestamp, 
	channel_flags,
	direction,
	disable,
	cltv_expiry_delta,
	htlc_minimum_msat,
	fee_base_msat,
	fee_proportional_millionths,
	htlc_maximum_msat,
	blob_signed
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)  ON CONFLICT DO NOTHING
                        """
                        )

                    sqlCommand.Parameters.AddWithValue scid
                    |> ignore<NpgsqlParameter>

                    sqlCommand.Parameters.AddWithValue timestamp
                    |> ignore<NpgsqlParameter>

                    sqlCommand.Parameters.AddWithValue(
                        int16 updateMsg.Contents.ChannelFlags
                    )
                    |> ignore<NpgsqlParameter>

                    sqlCommand.Parameters.AddWithValue direction
                    |> ignore<NpgsqlParameter>

                    sqlCommand.Parameters.AddWithValue disable
                    |> ignore<NpgsqlParameter>

                    sqlCommand.Parameters.AddWithValue cltvExpiryDelta
                    |> ignore<NpgsqlParameter>

                    sqlCommand.Parameters.AddWithValue htlcMinimumMsat
                    |> ignore<NpgsqlParameter>

                    sqlCommand.Parameters.AddWithValue feeBaseMsat
                    |> ignore<NpgsqlParameter>

                    sqlCommand.Parameters.AddWithValue feeProportionalMillionths
                    |> ignore<NpgsqlParameter>

                    sqlCommand.Parameters.AddWithValue htlcMaximumMsat
                    |> ignore<NpgsqlParameter>

                    sqlCommand.Parameters.AddWithValue bytes
                    |> ignore<NpgsqlParameter>

                    do!
                        sqlCommand.ExecuteNonQueryAsync()
                        |> Async.AwaitTask
                        |> Async.Ignore

                    Console.WriteLine(
                        sprintf
                            "Added update for channel #%i"
                            (updateMsg.Contents.ShortChannelId.ToUInt64())
                    )
                | FinishedInitialSync ->
                    Console.WriteLine(
                        "Finished persisting initial sync, notifying snapshotter..."
                    )

                    notifySnapshotter.Cancel()
                | _ -> ()

            return ()
        }
