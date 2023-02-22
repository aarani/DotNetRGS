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
        mutable Flags: bool
        mutable CltvExpiryDelta: bool
        mutable HtlcMinimumMSat: bool
        mutable FeeBaseMSat: bool
        mutable FeeProportionalMillionths: bool
        mutable HtlcMaximumMSat: bool
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
        mutable LastUpdateBeforeSeen: Option<UnsignedChannelUpdateMsg>
        mutable MutatedProperties: MutatedProperties
        mutable LastUpdateAfterSeen: Option<UpdateDelta>
    }

    static member Default =
        {
            LastUpdateBeforeSeen = None
            MutatedProperties = MutatedProperties.Default
            LastUpdateAfterSeen = None
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

type DeltaSet = Map<ShortChannelId, ChannelDelta>

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
        (deltaSet: DeltaSet)
        (lastSyncTimestamp: DateTime)
        =
        async {
            let rec readCurrentAnnouncements(deltaSet: DeltaSet) =
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
                            }

                        return! readRow deltaSet
                    else
                        return deltaSet
                }

            let! deltaSet = readChannelsWithNewUpdates deltaSet

            return deltaSet
        }

    let fetchChannelUpdates
        (deltaSet: DeltaSet)
        (lastSyncTimeStamp: DateTime)
        (considerIntermediateUpdates: bool)
        =
        async {
            let mutable last_seen_update_ids = List<int>.Empty
            let mutable non_intermediate_ids = Set<int>(Seq.empty)
            // get the latest channel update in each direction prior to last_sync_timestamp, provided
            // there was an update in either direction that happened after the last sync (to avoid
            // collecting too many reference updates)
            let readReference(deltaSet: DeltaSet) =
                async {
                    let readCommand =
                        dataSource.CreateCommand(
                            "SELECT DISTINCT ON (short_channel_id, direction) id, direction, blob_signed FROM channel_updates WHERE seen < $1 AND short_channel_id IN (SELECT short_channel_id FROM channel_updates WHERE seen >= $1 GROUP BY short_channel_id) ORDER BY short_channel_id ASC, direction ASC, seen DESC"
                        )

                    readCommand.Parameters.AddWithValue lastSyncTimeStamp
                    |> ignore<NpgsqlParameter>

                    let! reader =
                        readCommand.ExecuteReaderAsync() |> Async.AwaitTask

                    if reader.HasRows then
                        let rec readRow(deltaSet: DeltaSet) =
                            async {
                                let! readResult =
                                    reader.ReadAsync() |> Async.AwaitTask

                                if readResult then
                                    let updateId = reader.GetInt32 0

                                    last_seen_update_ids <-
                                        updateId :: last_seen_update_ids

                                    non_intermediate_ids <-
                                        Set.add updateId non_intermediate_ids

                                    let direction = reader.GetBoolean 1

                                    let! updateMsg =
                                        reader.GetStream 2
                                        |> EasyLightningReader.readLightningMsgFromStream<ChannelUpdateMsg>

                                    let scId = updateMsg.Contents.ShortChannelId

                                    let currentChannelDelta =
                                        deltaSet
                                        |> Map.tryFind scId
                                        |> Option.defaultValue
                                            ChannelDelta.Default

                                    let updates = currentChannelDelta.Updates

                                    let channelDelta =
                                        if not direction then
                                            { currentChannelDelta with
                                                Updates =
                                                    let update =
                                                        fst updates
                                                        |> Option.defaultValue
                                                            DirectedUpdateDelta.Default

                                                    Some
                                                        { update with
                                                            LastUpdateBeforeSeen =
                                                                Some
                                                                    updateMsg.Contents
                                                        },
                                                    snd updates
                                            }
                                        else
                                            { currentChannelDelta with
                                                Updates =
                                                    let update =
                                                        snd updates
                                                        |> Option.defaultValue
                                                            DirectedUpdateDelta.Default

                                                    fst updates,
                                                    Some
                                                        { update with
                                                            LastUpdateBeforeSeen =
                                                                Some
                                                                    updateMsg.Contents
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

            let! deltaSet = readReference deltaSet
            // get all the intermediate channel updates
            // (to calculate the set of mutated fields for snapshotting, where intermediate updates may
            // have been omitted)
            let readIntermediates(deltaSet: DeltaSet) =
                async {
                    let readCommand =
                        let prefix =
                            if not considerIntermediateUpdates then
                                "DISTINCT ON (short_channel_id, direction)"
                            else
                                ""

                        dataSource.CreateCommand(
                            sprintf
                                "SELECT %s id, direction, blob_signed, seen FROM channel_updates WHERE seen >= $1 ORDER BY short_channel_id ASC, direction ASC, seen DESC"
                                prefix
                        )

                    readCommand.Parameters.AddWithValue lastSyncTimeStamp
                    |> ignore<NpgsqlParameter>

                    let! reader =
                        readCommand.ExecuteReaderAsync() |> Async.AwaitTask

                    let mutable previousShortChannelId = UInt64.MaxValue
                    let mutable previouslySeenDirections = (false, false)

                    if reader.HasRows then
                        let rec readRow(deltaSet: DeltaSet) =
                            async {
                                let! readResult =
                                    reader.ReadAsync() |> Async.AwaitTask

                                if readResult then
                                    let updateId = reader.GetInt32 0

                                    if non_intermediate_ids.Contains updateId then
                                        return! readRow deltaSet
                                    else
                                        let direction = reader.GetBoolean 1

                                        let! updateMsg =
                                            reader.GetStream 2
                                            |> EasyLightningReader.readLightningMsgFromStream<ChannelUpdateMsg>

                                        let scId =
                                            updateMsg.Contents.ShortChannelId

                                        let currentSeenTimestamp =
                                            reader.GetDateTime 3

                                        let currentChannelDelta =
                                            deltaSet
                                            |> Map.tryFind scId
                                            |> Option.defaultValue
                                                ChannelDelta.Default

                                        let updates =
                                            currentChannelDelta.Updates

                                        let updateDelta =
                                            if not direction then
                                                fst updates
                                                |> Option.defaultValue
                                                    DirectedUpdateDelta.Default
                                            else
                                                snd updates
                                                |> Option.defaultValue
                                                    DirectedUpdateDelta.Default

                                        if
                                            not direction
                                            && not(fst previouslySeenDirections)
                                        then
                                            previouslySeenDirections <-
                                                true,
                                                snd previouslySeenDirections

                                            updateDelta.LastUpdateAfterSeen <-
                                                Some
                                                    {
                                                        Seen =
                                                            DateTimeUtils.ToUnixTimestamp
                                                                currentSeenTimestamp
                                                        Update =
                                                            updateMsg.Contents
                                                    }

                                        else
                                            previouslySeenDirections <-
                                                fst previouslySeenDirections,
                                                true

                                            updateDelta.LastUpdateAfterSeen <-
                                                Some
                                                    {
                                                        Seen =
                                                            DateTimeUtils.ToUnixTimestamp
                                                                currentSeenTimestamp
                                                        Update =
                                                            updateMsg.Contents
                                                    }

                                        let lastSeenUpdate =
                                            updateDelta.LastUpdateBeforeSeen

                                        if lastSeenUpdate.IsSome then
                                            let lastSeenUpdate =
                                                lastSeenUpdate.Value

                                            if updateMsg.Contents.ChannelFlags
                                               <> lastSeenUpdate.ChannelFlags then
                                                updateDelta.MutatedProperties.Flags <-
                                                    true

                                            if updateMsg.Contents.CLTVExpiryDelta
                                               <> lastSeenUpdate.CLTVExpiryDelta then
                                                updateDelta.MutatedProperties.CltvExpiryDelta <-
                                                    true

                                            if updateMsg.Contents.HTLCMinimumMSat
                                               <> lastSeenUpdate.HTLCMinimumMSat then
                                                updateDelta.MutatedProperties.HtlcMinimumMSat <-
                                                    true

                                            if updateMsg.Contents.FeeBaseMSat
                                               <> lastSeenUpdate.FeeBaseMSat then
                                                updateDelta.MutatedProperties.FeeBaseMSat <-
                                                    true

                                            if updateMsg.Contents.FeeProportionalMillionths
                                               <> lastSeenUpdate.FeeProportionalMillionths then
                                                updateDelta.MutatedProperties.FeeProportionalMillionths <-
                                                    true

                                            if updateMsg.Contents.HTLCMaximumMSat
                                               <> lastSeenUpdate.HTLCMaximumMSat then
                                                updateDelta.MutatedProperties.HtlcMaximumMSat <-
                                                    true

                                        let channelDelta =
                                            if not direction then
                                                { currentChannelDelta with
                                                    Updates =
                                                        Some updateDelta,
                                                        snd updates
                                                }
                                            else
                                                { currentChannelDelta with
                                                    Updates =
                                                        fst updates,
                                                        Some updateDelta
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

            let! deltaSet = readIntermediates deltaSet

            return deltaSet
        }

    let filterDeltaSet(deltaSet: DeltaSet) =
        let rec filter
            (deltaSet: List<ShortChannelId * ChannelDelta>)
            (state: List<ShortChannelId * ChannelDelta>)
            : List<ShortChannelId * ChannelDelta> =
            match deltaSet with
            | (scId, delta) :: tail ->
                //FIXME: this doesn't really do anything because we don't have network graph
                // like original rgs does. pruning is an issue which needs to be solved.
                if delta.Announcement.IsNone then
                    filter tail state
                else
                    let updateMeetsCriteria
                        (update: Option<DirectedUpdateDelta>)
                        =
                        if update.IsNone then
                            false
                        else
                            update.Value.LastUpdateAfterSeen.IsSome

                    if updateMeetsCriteria(fst delta.Updates) |> not
                       && updateMeetsCriteria(snd delta.Updates) |> not then
                        filter tail state
                    else
                        filter tail ((scId, delta)::state)
            | [] -> state

        filter (deltaSet |> Map.toList) List.empty |> Map.ofList

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

            let deltaSet = DeltaSet(Seq.empty)

            let! deltaSet =
                fetchChannelAnnouncements deltaSet (DateTime.Today.AddHours -5.)

            let! deltaSet =
                fetchChannelUpdates deltaSet (DateTime.Today.AddHours -5.) true

            let deltaSet = filterDeltaSet deltaSet

            Console.WriteLine(deltaSet.Count)
            return ()
        }



    member self.Start() =
        async {
            do! startToken.WaitHandle |> Async.AwaitWaitHandle |> Async.Ignore

            let rec snapshot() =
                async {
                    //TODO: create snapshots
                    do! self.SerializeDelta()

                    let nextSnapshot = DateTime.UtcNow.Date.AddDays 1.

                    // sleep until next day
                    // FIXME: is it possible that this cause problem when we're exteremely close to the end of day? (negetive timespan and etc)
                    do! Async.Sleep(nextSnapshot.Subtract DateTime.UtcNow)
                    return! snapshot()
                }

            do! snapshot()
        }
