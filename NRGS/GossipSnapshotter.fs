namespace NRGS

open System
open System.IO
open System.Threading

open NBitcoin

open DotNetLightning.Serialization.Msgs
open DotNetLightning.Utils

open ResultUtils.Portability

open Npgsql

type MutatedProperties =
    {
        Flags: bool
        CltvExpiryDelta: bool
        HtlcMinimumMSat: bool
        FeeBaseMSat: bool
        FeeProportionalMillionths: bool
        HtlcMaximumMSat: bool
    }

    static member Default =
        {
            Flags = false
            CltvExpiryDelta = false
            HtlcMinimumMSat = false
            FeeBaseMSat = false
            FeeProportionalMillionths = false
            HtlcMaximumMSat = false
        }

type AnnouncementDelta =
    {
        Seen: uint32
        Announcement: UnsignedChannelAnnouncementMsg
    }

type UpdateDelta =
    {
        Seen: uint32
        Update: UnsignedChannelUpdateMsg
    }

type DirectedUpdateDelta =
    {
        LastUpdateBeforeSeen: Option<UnsignedChannelUpdateMsg>
        MutatedProperties: MutatedProperties
        LastUpdateAfterSeen: Option<UpdateDelta>
    }

type ChannelDelta =
    {
        Announcement: Option<AnnouncementDelta>
        Updates: Option<DirectedUpdateDelta> * Option<DirectedUpdateDelta>
        FirstUpdateSeen: Option<uint32>
    }

    static member Default =
        {
            Announcement = None
            Updates = (None, None)
            FirstUpdateSeen = None
        }

module EasyLightningReader =
    let readStreamToEnd(stream: Stream) =
        async {
            use memStream = new MemoryStream()

            do! stream.CopyToAsync(memStream) |> Async.AwaitTask

            return memStream.ToArray()
        }

    let readLightningMsgFromStream<'T when 'T :> ILightningMsg>
        (stream: Stream)
        : Async<'T> =
        async {
            let! bytes = readStreamToEnd stream

            match LightningMsg.fromBytes bytes with
            | Ok(:? 'T as msg) -> return msg
            | _ -> return failwith "how the fuck we have an invalid msg in DB"
        }


type GossipSnapshotter(startToken: CancellationToken) =
    let dataSource =
        NpgsqlDataSource.Create(
            "Host=127.0.0.1;Username=postgres;Password=f50d47dc6afe40918afa2a935637ec1e;Database=nrgs"
        )

    let fetchChannelAnnouncements
        (
            deltaSet: Map<ShortChannelId, ChannelDelta>,
            lastSyncTimestamp: DateTime
        ) =
        async {
            let rec readCurrentAnnouncements
                (deltaSet: Map<ShortChannelId, ChannelDelta>)
                =
                async {
                    let readAnns =
                        dataSource.CreateCommand
                            "SELECT announcement_signed, seen FROM channel_announcements ORDER BY short_channel_id ASC"

                    let! reader =
                        readAnns.ExecuteReaderAsync() |> Async.AwaitTask

                    if reader.HasRows then
                        let rec readRow deltaSet =
                            async {
                                let! readResult =
                                    reader.ReadAsync() |> Async.AwaitTask

                                if readResult then
                                    let! annSigned =
                                        reader.GetStream 0
                                        |> EasyLightningReader.readLightningMsgFromStream<ChannelAnnouncementMsg>

                                    let scId = annSigned.Contents.ShortChannelId

                                    let currentSeenTimestamp =
                                        DateTimeUtils.ToUnixTimestamp(
                                            reader.GetDateTime 1
                                        )

                                    let previousOrDefault =
                                        deltaSet
                                        |> Map.tryFind scId
                                        |> Option.defaultValue
                                            ChannelDelta.Default

                                    let channelDelta =
                                        { previousOrDefault with
                                            Announcement =
                                                Some
                                                    {
                                                        AnnouncementDelta.Announcement =
                                                            annSigned.Contents
                                                        Seen =
                                                            currentSeenTimestamp
                                                    }
                                        }

                                    return!
                                        readRow(
                                            deltaSet
                                            |> Map.add scId channelDelta
                                        )
                                else
                                    return deltaSet
                            }

                        return! readRow deltaSet
                    else
                        return deltaSet
                }

            let! deltaSet = readCurrentAnnouncements deltaSet

            // here is where the channels whose first update in either direction occurred after
            // `last_seen_timestamp` are added to the selection
            let rec readChannelsWithNewUpdates
                (deltaSet: Map<ShortChannelId, ChannelDelta>)
                =
                async {
                    let readNewUpdates =
                        dataSource.CreateCommand
                            "SELECT blob_signed, seen FROM (SELECT DISTINCT ON (short_channel_id) short_channel_id, blob_signed, seen FROM channel_updates ORDER BY short_channel_id ASC, seen ASC) AS first_seens WHERE first_seens.seen >= $1"

                    readNewUpdates.Parameters.AddWithValue lastSyncTimestamp
                    |> ignore<NpgsqlParameter>

                    let! reader =
                        readNewUpdates.ExecuteReaderAsync() |> Async.AwaitTask

                    if reader.HasRows then
                        let rec readRow deltaSet =
                            async {
                                let! readResult =
                                    reader.ReadAsync() |> Async.AwaitTask

                                if readResult then
                                    let! update =
                                        reader.GetStream 0
                                        |> EasyLightningReader.readLightningMsgFromStream<ChannelUpdateMsg>

                                    let scId = update.Contents.ShortChannelId

                                    let currentSeenTimestamp =
                                        reader.GetDateTime 1
                                        |> DateTimeUtils.ToUnixTimestamp

                                    let previousOrDefault =
                                        deltaSet
                                        |> Map.tryFind scId
                                        |> Option.defaultValue
                                            ChannelDelta.Default

                                    let channelDelta =
                                        { previousOrDefault with
                                            FirstUpdateSeen =
                                                Some currentSeenTimestamp
                                        }

                                    return!
                                        readRow(
                                            deltaSet
                                            |> Map.add scId channelDelta
                                        )
                                else
                                    return deltaSet
                            }   `

                        return! readRow deltaSet
                    else
                        return deltaSet
                }

            let! deltaSet = readChannelsWithNewUpdates deltaSet

            return deltaSet
        }

    member __.SerializeDelta() =
        async {
            let mutable nodeIdsSet = Set<array<byte>>(Seq.empty)
            let mutable nodeIds = List<PubKey>.Empty
            let mutable nodeIdsIndices = Map<array<byte>, int>(Seq.empty)

            let getNodeIdIndex(pubKey: PubKey) =
                let serializedNodeId = pubKey.ToBytes()

                if nodeIdsSet |> Set.contains serializedNodeId |> not then
                    nodeIdsSet <- nodeIdsSet.Add serializedNodeId
                    nodeIds <- nodeIds @ List.singleton pubKey
                    let index = nodeIds.Length - 1

                    nodeIdsIndices <-
                        Map.add serializedNodeId index nodeIdsIndices

                    index
                else
                    nodeIdsIndices.[serializedNodeId]

            let deltaSet = Map<ShortChannelId, ChannelDelta>(Seq.empty)
            let! deltaSet = fetchChannelAnnouncements deltaSet

            return ()
        }

    member __.Start() =
        async {
            do! startToken.WaitHandle |> Async.AwaitWaitHandle |> Async.Ignore

            let rec snapshot() =
                async {
                    //TODO: create snapshots


                    let nextSnapshot = DateTime.UtcNow.Date.AddDays 1.

                    // sleep until next day
                    // FIXME: is it possible that this cause problem when we're exteremely close to the end of day? (negetive timespan and etc)
                    do! Async.Sleep(nextSnapshot.Subtract DateTime.UtcNow)
                    return! snapshot()
                }

            do! snapshot()
        }
