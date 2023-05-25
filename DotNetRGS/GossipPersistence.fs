namespace DotNetRGS

open System
open System.Threading
open System.Threading.Tasks.Dataflow

open DotNetLightning.Serialization.Msgs
open DotNetLightning.Utils

open Npgsql

open Microsoft.Extensions.Configuration

type internal GossipPersistence
    (
        configuration: IConfiguration,
        verifiedMsgHandler: BufferBlock<Message>,
        graph: NetworkGraph,
        notifySnapshotter: CancellationTokenSource
    ) =

    member self.Start() =
        async {
            let! cancelToken = Async.CancellationToken
            cancelToken.ThrowIfCancellationRequested()

            let connectionString = configuration.GetConnectionString("MainDB")

            use dataSource = NpgsqlDataSource.Create connectionString

            use initializeCommand =
                dataSource.CreateCommand(
                    """
CREATE TABLE IF NOT EXISTS snapshots
(
    id SERIAL PRIMARY KEY,
    "referenceDateTime" timestamp NOT NULL DEFAULT NOW(),
    blob bytea NOT NULL,
    "dayRange" integer NOT NULL,
    "lastSyncTimestamp" timestamp NOT NULL
);
CREATE TABLE IF NOT EXISTS channel_announcements (
	id SERIAL PRIMARY KEY,
	short_channel_id bigint NOT NULL UNIQUE,
	announcement_signed BYTEA,
	seen timestamp NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS channel_updates (
	id SERIAL PRIMARY KEY,
	short_channel_id bigint NOT NULL,
	timestamp bigint NOT NULL,
	channel_flags smallint NOT NULL,
	direction boolean NOT NULL,
	disable boolean NOT NULL,
	cltv_expiry_delta integer NOT NULL,
	htlc_minimum_msat bigint NOT NULL,
	fee_base_msat integer NOT NULL,
	fee_proportional_millionths integer NOT NULL,
	htlc_maximum_msat bigint NOT NULL,
	blob_signed BYTEA NOT NULL,
	seen timestamp NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS channel_updates_seen ON channel_updates(seen, short_channel_id, direction) INCLUDE (id, blob_signed);
CREATE INDEX IF NOT EXISTS channel_updates_scid_seen ON channel_updates(short_channel_id, seen) INCLUDE (blob_signed);
CREATE INDEX IF NOT EXISTS channel_updates_seen_scid ON channel_updates(seen, short_channel_id);
CREATE INDEX IF NOT EXISTS channel_updates_scid_dir_seen ON channel_updates(short_channel_id ASC, direction ASC, seen DESC) INCLUDE (id, blob_signed);
CREATE UNIQUE INDEX IF NOT EXISTS channel_updates_key ON channel_updates (short_channel_id, direction, timestamp);
ALTER TABLE channel_updates SET ( autovacuum_vacuum_insert_scale_factor = 0.005 );
ALTER TABLE channel_announcements SET ( autovacuum_vacuum_insert_scale_factor = 0.005 );
                """
                )

            do!
                initializeCommand.ExecuteNonQueryAsync()
                |> Async.AwaitTask
                |> Async.Ignore

            let mutable lastGraphSaveTime = DateTime.MinValue
            let graphSaveInterval = TimeSpan.FromMinutes 10.

            while true do
                let! msg =
                    verifiedMsgHandler.ReceiveAsync cancelToken
                    |> Async.AwaitTask

                if lastGraphSaveTime < DateTime.UtcNow - graphSaveInterval then
                    graph.Save()
                    lastGraphSaveTime <- DateTime.UtcNow

                match msg with
                | VerifiedChannelAnnouncement(channelAnn, capacityOpt, bytes) ->
                    use sqlCommand =
                        dataSource.CreateCommand
                            "INSERT INTO channel_announcements (short_channel_id, announcement_signed) VALUES ($1, $2) ON CONFLICT (short_channel_id) DO NOTHING"

                    // NpgSql doesn't support uint64 so we need to cast it to int64
                    channelAnn.Contents.ShortChannelId.ToUInt64()
                    |> int64
                    |> sqlCommand.Parameters.AddWithValue
                    |> ignore<NpgsqlParameter>

                    sqlCommand.Parameters.AddWithValue bytes
                    |> ignore<NpgsqlParameter>

                    do!
                        sqlCommand.ExecuteNonQueryAsync()
                        |> Async.AwaitTask
                        |> Async.Ignore

                    Logger.Log
                        "GossipPersistence"
                        (sprintf
                            "Added channel #%i"
                            (channelAnn.Contents.ShortChannelId.ToUInt64()))

                    graph.AddChannel channelAnn.Contents capacityOpt
                | RoutingMsg(:? ChannelUpdateMsg as updateMsg, bytes) ->
                    if graph.AddChannelUpdate updateMsg then
                        let scid =
                            updateMsg.Contents.ShortChannelId.ToUInt64()
                            |> int64

                        let timestamp = updateMsg.Contents.Timestamp |> int64

                        let direction =
                            (updateMsg.Contents.ChannelFlags &&& 1uy) = 1uy

                        let disable =
                            (updateMsg.Contents.ChannelFlags &&& 2uy) > 0uy

                        let cltvExpiryDelta =
                            updateMsg.Contents.CLTVExpiryDelta.Value |> int

                        let htlcMinimumMsat =
                            updateMsg.Contents.HTLCMinimumMSat.MilliSatoshi
                            |> int64

                        let feeBaseMsat =
                            updateMsg.Contents.FeeBaseMSat.MilliSatoshi |> int

                        let feeProportionalMillionths =
                            updateMsg.Contents.FeeProportionalMillionths |> int

                        let htlcMaximumMsat =
                            updateMsg.Contents.HTLCMaximumMSat.Value.MilliSatoshi
                            |> int64

                        use sqlCommand =
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

                        sqlCommand.Parameters.AddWithValue
                            feeProportionalMillionths
                        |> ignore<NpgsqlParameter>

                        sqlCommand.Parameters.AddWithValue htlcMaximumMsat
                        |> ignore<NpgsqlParameter>

                        sqlCommand.Parameters.AddWithValue bytes
                        |> ignore<NpgsqlParameter>

                        do!
                            sqlCommand.ExecuteNonQueryAsync()
                            |> Async.AwaitTask
                            |> Async.Ignore

                        Logger.Log
                            "GossipPersistence"
                            (sprintf
                                "Added update for channel #%i"
                                (updateMsg.Contents.ShortChannelId.ToUInt64()))
                | FinishedInitialSync ->
                    graph.Save()

                    Logger.Log
                        "GossipPersistence"
                        "Finished persisting initial sync, notifying snapshotter..."

                    notifySnapshotter.Cancel()
                | _ -> ()

            return ()
        }
