﻿namespace DotNetRGS

open System
open System.Diagnostics
open System.IO
open System.Threading

open NBitcoin

open DotNetLightning.Serialization
open DotNetLightning.Serialization.Msgs
open DotNetLightning.Utils

open ResultUtils.Portability

open Npgsql

open Microsoft.Extensions.Configuration

open DotNetRGS.Utils
open DotNetRGS.Utils.FSharpUtil

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

    /// Does not include flags because the flag byte is always sent in full
    member self.Length() =
        let boolToInt =
            function
            | true -> 1
            | false -> 0

        boolToInt self.CltvExpiryDelta
        + boolToInt self.HtlcMinimumMSat
        + boolToInt self.FeeBaseMSat
        + boolToInt self.FeeProportionalMillionths
        + boolToInt self.HtlcMaximumMSat

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
            Updates = None, None
            FirstUpdateSeen = None
        }

type DeltaSet = Map<ShortChannelId, ChannelDelta>

type ChainHash = uint256

type DefaultUpdateValues =
    {
        CLTVExpiryDelta: BlockHeightOffset16
        HTLCMinimumMSat: LNMoney
        FeeBaseMSat: LNMoney
        FeeProportionalMillionths: uint32
        HTLCMaximumMSat: LNMoney
    }

    static member Default =
        {
            CLTVExpiryDelta = BlockHeightOffset16.Zero
            HTLCMinimumMSat = LNMoney.Zero
            FeeBaseMSat = LNMoney.Zero
            FeeProportionalMillionths = 0u
            HTLCMaximumMSat = LNMoney.Zero
        }

type UpdateSerializationMechanism =
    | Full
    | Incremental of MutatedProperties

type UpdateSerialization =
    {
        Update: UnsignedChannelUpdateMsg
        Mechanism: UpdateSerializationMechanism
    }

type SerializationSet =
    {
        Announcements: List<UnsignedChannelAnnouncementMsg>
        Updates: List<UpdateSerialization>
        FullUpdateDefaults: DefaultUpdateValues
        LatestSeen: uint32
        ChainHash: ChainHash
    }

type FullUpdateValueHistograms =
    {
        CLTVExpiryDelta: Histogram<BlockHeightOffset16>
        HTLCMinimumMSat: Histogram<LNMoney>
        FeeBaseMSat: Histogram<LNMoney>
        FeeProportionalMillionths: Histogram<uint32>
        HTLCMaximumMSat: Histogram<LNMoney>
    }

    static member Default =
        {
            CLTVExpiryDelta = Histogram.empty
            HTLCMinimumMSat = Histogram.empty
            FeeBaseMSat = Histogram.empty
            FeeProportionalMillionths = Histogram.empty
            HTLCMaximumMSat = Histogram.empty
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

type GossipSnapshotter
    (
        configuration: IConfiguration,
        networkGraph: NetworkGraph,
        startToken: CancellationToken
    ) =
    let dataSource =
        NpgsqlDataSource.Create(configuration.GetConnectionString("MainDB"))

    let fetchChannelAnnouncements
        (deltaSet: DeltaSet)
        (networkGraph: NetworkGraph)
        (lastSyncTimestamp: DateTime)
        =
        async {
            let rec readCurrentAnnouncements(deltaSet: DeltaSet) =
                async {
                    use readAnns =
                        dataSource.CreateCommand
                            "SELECT announcement_signed, seen FROM channel_announcements WHERE short_channel_id = any($1) ORDER BY short_channel_id ASC"

                    networkGraph.GetChannelIds()
                    |> Array.map(fun scId -> scId.ToUInt64() |> int64)
                    |> readAnns.Parameters.AddWithValue
                    |> ignore<NpgsqlParameter>

                    use reader = readAnns.ExecuteReader()

                    if reader.HasRows then
                        let rec readRow deltaSet =
                            async {
                                let readResult = reader.Read()

                                if readResult then
                                    let! annSigned =
                                        reader.GetOrdinal "announcement_signed"
                                        |> reader.GetStream
                                        |> EasyLightningReader.readLightningMsgFromStream<ChannelAnnouncementMsg>

                                    let scId = annSigned.Contents.ShortChannelId

                                    let currentSeenTimestamp =
                                        reader.GetOrdinal "seen"
                                        |> reader.GetDateTime
                                        |> DateTimeUtils.ToUnixTimestamp

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
                                        deltaSet
                                        |> Map.add scId channelDelta
                                        |> readRow
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
                    use readNewUpdates =
                        dataSource.CreateCommand
                            "SELECT blob_signed, seen FROM (SELECT DISTINCT ON (short_channel_id) short_channel_id, blob_signed, seen FROM channel_updates ORDER BY short_channel_id ASC, seen ASC) AS first_seens WHERE first_seens.seen >= $1"

                    readNewUpdates.Parameters.AddWithValue lastSyncTimestamp
                    |> ignore<NpgsqlParameter>

                    use reader = readNewUpdates.ExecuteReader()

                    if reader.HasRows then
                        let rec readRow deltaSet =
                            async {
                                let readResult = reader.Read()

                                if readResult then
                                    let! update =
                                        reader.GetOrdinal "blob_signed"
                                        |> reader.GetStream
                                        |> EasyLightningReader.readLightningMsgFromStream<ChannelUpdateMsg>

                                    let scId = update.Contents.ShortChannelId

                                    let currentSeenTimestamp =
                                        reader.GetOrdinal "seen"
                                        |> reader.GetDateTime
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
                                        deltaSet
                                        |> Map.add scId channelDelta
                                        |> readRow
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
            let emptySet = Set<int> Seq.empty

            // get the latest channel update in each direction prior to last_sync_timestamp, provided
            // there was an update in either direction that happened after the last sync (to avoid
            // collecting too many reference updates)
            let readReferences(deltaSet: DeltaSet) =
                async {
                    use readCommand =
                        dataSource.CreateCommand(
                            "SELECT DISTINCT ON (short_channel_id, direction) id, direction, blob_signed FROM channel_updates WHERE seen < $1 AND short_channel_id IN (SELECT short_channel_id FROM channel_updates WHERE seen >= $1 GROUP BY short_channel_id) ORDER BY short_channel_id ASC, direction ASC, seen DESC"
                        )

                    readCommand.Parameters.AddWithValue lastSyncTimeStamp
                    |> ignore<NpgsqlParameter>

                    use reader = readCommand.ExecuteReader()

                    if reader.HasRows then
                        let rec innerReadReferences
                            (referenceIds: Set<int>)
                            (deltaSet: DeltaSet)
                            =
                            async {
                                let readResult = reader.Read()

                                if readResult then
                                    let updateId =
                                        reader.GetOrdinal "id"
                                        |> reader.GetInt32

                                    let direction =
                                        reader.GetOrdinal "direction"
                                        |> reader.GetBoolean

                                    let! updateMsg =
                                        reader.GetOrdinal "blob_signed"
                                        |> reader.GetStream
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
                                        deltaSet
                                        |> Map.add scId channelDelta
                                        |> innerReadReferences(
                                            Set.add updateId referenceIds
                                        )
                                else
                                    return deltaSet, referenceIds
                            }

                        return! innerReadReferences emptySet deltaSet
                    else
                        return deltaSet, emptySet
                }

            let! deltaSet, referenceIds = readReferences deltaSet
            // get all the intermediate channel updates
            // (to calculate the set of mutated fields for snapshotting, where intermediate updates may
            // have been omitted)
            let readIntermediates
                (deltaSet: DeltaSet)
                (referenceIds: Set<int>)
                =
                async {
                    use readCommand =
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

                    use reader = readCommand.ExecuteReader()

                    if reader.HasRows then
                        let rec readRow
                            (previousShortChannelId: ShortChannelId)
                            (previouslySeenDirections: bool * bool)
                            (deltaSet: DeltaSet)
                            =
                            async {
                                let readResult = reader.Read()

                                if readResult then
                                    let updateId =
                                        reader.GetOrdinal "id"
                                        |> reader.GetInt32

                                    if referenceIds.Contains updateId then
                                        return!
                                            readRow
                                                previousShortChannelId
                                                previouslySeenDirections
                                                deltaSet
                                    else
                                        let direction =
                                            reader.GetOrdinal "direction"
                                            |> reader.GetBoolean

                                        let! updateMsg =
                                            reader.GetOrdinal "blob_signed"
                                            |> reader.GetStream
                                            |> EasyLightningReader.readLightningMsgFromStream<ChannelUpdateMsg>

                                        let scId =
                                            updateMsg.Contents.ShortChannelId

                                        let (previousShortChannelId,
                                             previouslySeenDirections) =
                                            if previousShortChannelId <> scId then
                                                scId, (false, false)
                                            else
                                                previousShortChannelId,
                                                previouslySeenDirections

                                        let currentSeenTimestamp =
                                            reader.GetOrdinal "seen"
                                            |> reader.GetDateTime

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

                                        let (previouslySeenDirections,
                                             updateDelta) =
                                            if
                                                not direction
                                                && not
                                                    (
                                                        fst
                                                            previouslySeenDirections
                                                    )
                                            then
                                                let previouslySeenDirections =
                                                    true,
                                                    snd previouslySeenDirections

                                                previouslySeenDirections,
                                                { updateDelta with
                                                    LastUpdateAfterSeen =
                                                        Some
                                                            {
                                                                Seen =
                                                                    DateTimeUtils.ToUnixTimestamp
                                                                        currentSeenTimestamp
                                                                Update =
                                                                    updateMsg.Contents
                                                            }
                                                }
                                            elif
                                                direction
                                                && not
                                                    (
                                                        snd
                                                            previouslySeenDirections
                                                    )
                                            then
                                                let previouslySeenDirections =
                                                    fst previouslySeenDirections,
                                                    true

                                                previouslySeenDirections,
                                                { updateDelta with
                                                    LastUpdateAfterSeen =
                                                        Some
                                                            {
                                                                Seen =
                                                                    DateTimeUtils.ToUnixTimestamp
                                                                        currentSeenTimestamp
                                                                Update =
                                                                    updateMsg.Contents
                                                            }
                                                }
                                            else
                                                previouslySeenDirections,
                                                updateDelta

                                        let updateDelta =
                                            match
                                                updateDelta.LastUpdateBeforeSeen
                                                with
                                            | Some lastSeenUpdate ->
                                                { updateDelta with
                                                    MutatedProperties =
                                                        { MutatedProperties.Default with
                                                            Flags =
                                                                updateMsg.Contents.ChannelFlags
                                                                <> lastSeenUpdate.ChannelFlags
                                                            CltvExpiryDelta =
                                                                updateMsg.Contents.CLTVExpiryDelta
                                                                <> lastSeenUpdate.CLTVExpiryDelta
                                                            HtlcMinimumMSat =
                                                                updateMsg.Contents.HTLCMinimumMSat
                                                                <> lastSeenUpdate.HTLCMinimumMSat
                                                            FeeBaseMSat =
                                                                updateMsg.Contents.FeeBaseMSat
                                                                <> lastSeenUpdate.FeeBaseMSat
                                                            FeeProportionalMillionths =
                                                                updateMsg.Contents.FeeProportionalMillionths
                                                                <> lastSeenUpdate.FeeProportionalMillionths
                                                            HtlcMaximumMSat =
                                                                updateMsg.Contents.HTLCMaximumMSat
                                                                <> lastSeenUpdate.HTLCMaximumMSat
                                                        }
                                                }
                                            | None -> updateDelta

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
                                            deltaSet
                                            |> Map.add scId channelDelta
                                            |> readRow
                                                previousShortChannelId
                                                previouslySeenDirections

                                else
                                    return deltaSet
                            }

                        return!
                            readRow
                                (ShortChannelId.FromUInt64 UInt64.MaxValue)
                                (false, false)
                                deltaSet
                    else
                        return deltaSet
                }

            let! deltaSet = readIntermediates deltaSet referenceIds

            return deltaSet
        }

    let filterDeltaSet(deltaSet: DeltaSet) =
        let rec filter
            (deltaSet: List<ShortChannelId * ChannelDelta>)
            (state: List<ShortChannelId * ChannelDelta>)
            : List<ShortChannelId * ChannelDelta> =
            match deltaSet with
            | (scId, delta) :: tail ->
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
                        filter tail ((scId, delta) :: state)
            | [] -> state

        filter (deltaSet |> Map.toList) List.empty |> Map.ofList

    let serializeDeltaSet (deltaSet: DeltaSet) (lastSyncTimestamp: uint32) =
        let serializationSet =
            {
                Announcements = List.Empty
                Updates = List.Empty
                FullUpdateDefaults = DefaultUpdateValues.Default
                LatestSeen = 0u
                ChainHash = Constants.ChainHash
            }

        let rec readChannelDelta
            (channelDeltas: List<ShortChannelId * ChannelDelta>)
            (serializationSet: SerializationSet)
            (fullUpdateHistograms: FullUpdateValueHistograms)
            =
            match channelDeltas with
            | (_scId, channelDelta) :: tail ->
                let channelAnnouncementDelta =
                    UnwrapOption
                        channelDelta.Announcement
                        "channelDelta.Announcement is none, did you forget to run filterDeltaSet?"

                let currentAnnouncementSeen = channelAnnouncementDelta.Seen

                let isNewAnnouncement =
                    currentAnnouncementSeen >= lastSyncTimestamp

                let isNewlyUpdatedAnnouncement =
                    match channelDelta.FirstUpdateSeen with
                    | Some firstUpdateSeen ->
                        firstUpdateSeen >= lastSyncTimestamp
                    | None -> false

                let sendAnnouncement =
                    isNewAnnouncement || isNewlyUpdatedAnnouncement

                let serializationSet =
                    if sendAnnouncement then
                        { serializationSet with
                            LatestSeen =
                                max
                                    serializationSet.LatestSeen
                                    currentAnnouncementSeen
                            Announcements =
                                channelAnnouncementDelta.Announcement
                                :: serializationSet.Announcements
                        }
                    else
                        serializationSet

                let directionAUpdates, directionBUpdates = channelDelta.Updates

                let categorizeDirectedUpdateSerialization
                    (directedUpdates: Option<DirectedUpdateDelta>)
                    (serializationSet: SerializationSet)
                    (fullUpdateHistograms: FullUpdateValueHistograms)
                    =
                    let recordFullUpdateInHistograms
                        (fullUpdate: UnsignedChannelUpdateMsg)
                        (fullUpdateHistograms: FullUpdateValueHistograms)
                        =
                        let fullUpdateHistograms =
                            { fullUpdateHistograms with
                                CLTVExpiryDelta =
                                    fullUpdateHistograms.CLTVExpiryDelta
                                    |> Map.change
                                        fullUpdate.CLTVExpiryDelta
                                        (fun previousValue ->
                                            Some(
                                                (Option.defaultValue
                                                    0u
                                                    previousValue)
                                                + 1u
                                            )
                                        )
                            }

                        let fullUpdateHistograms =
                            { fullUpdateHistograms with
                                HTLCMinimumMSat =
                                    fullUpdateHistograms.HTLCMinimumMSat
                                    |> Map.change
                                        fullUpdate.HTLCMinimumMSat
                                        (fun previousValue ->
                                            Some(
                                                (Option.defaultValue
                                                    0u
                                                    previousValue)
                                                + 1u
                                            )
                                        )
                            }

                        let fullUpdateHistograms =
                            { fullUpdateHistograms with
                                FeeBaseMSat =
                                    fullUpdateHistograms.FeeBaseMSat
                                    |> Map.change
                                        fullUpdate.FeeBaseMSat
                                        (fun previousValue ->
                                            Some(
                                                (Option.defaultValue
                                                    0u
                                                    previousValue)
                                                + 1u
                                            )
                                        )
                            }

                        let fullUpdateHistograms =
                            { fullUpdateHistograms with
                                FeeProportionalMillionths =
                                    fullUpdateHistograms.FeeProportionalMillionths
                                    |> Map.change
                                        fullUpdate.FeeProportionalMillionths
                                        (fun previousValue ->
                                            Some(
                                                (Option.defaultValue
                                                    0u
                                                    previousValue)
                                                + 1u
                                            )
                                        )
                            }

                        let fullUpdateHistograms =
                            { fullUpdateHistograms with
                                HTLCMaximumMSat =
                                    fullUpdateHistograms.HTLCMaximumMSat
                                    |> Map.change
                                        fullUpdate.HTLCMaximumMSat.Value
                                        (fun previousValue ->
                                            Some(
                                                (Option.defaultValue
                                                    0u
                                                    previousValue)
                                                + 1u
                                            )
                                        )
                            }

                        fullUpdateHistograms

                    match directedUpdates with
                    | Some updates ->
                        match updates.LastUpdateAfterSeen with
                        | Some latestUpdateDelta ->
                            let latestUpdate = latestUpdateDelta.Update

                            let serializationSet =
                                { serializationSet with
                                    LatestSeen =
                                        max
                                            serializationSet.LatestSeen
                                            latestUpdateDelta.Seen
                                }

                            if updates.LastUpdateBeforeSeen.IsSome then
                                let mutatedProperties =
                                    updates.MutatedProperties

                                if mutatedProperties.Length() = 5 then
                                    // All five values have changed, it makes more sense to just
                                    // serialize the update as a full update instead of as a change
                                    // this way, the default values can be computed more efficiently
                                    let fullUpdateHistograms =
                                        recordFullUpdateInHistograms
                                            latestUpdate
                                            fullUpdateHistograms

                                    { serializationSet with
                                        Updates =
                                            {
                                                Update = latestUpdate
                                                Mechanism =
                                                    UpdateSerializationMechanism.Full
                                            }
                                            :: serializationSet.Updates
                                    },
                                    fullUpdateHistograms
                                elif mutatedProperties.Length() > 0
                                     || mutatedProperties.Flags then
                                    // We don't count flags as mutated properties
                                    { serializationSet with
                                        Updates =
                                            {
                                                Update = latestUpdate
                                                Mechanism =
                                                    UpdateSerializationMechanism.Incremental
                                                        mutatedProperties
                                            }
                                            :: serializationSet.Updates
                                    },
                                    fullUpdateHistograms
                                else
                                    serializationSet, fullUpdateHistograms
                            else
                                let fullUpdateHistograms =
                                    recordFullUpdateInHistograms
                                        latestUpdate
                                        fullUpdateHistograms

                                { serializationSet with
                                    Updates =
                                        {
                                            Update = latestUpdate
                                            Mechanism =
                                                UpdateSerializationMechanism.Full
                                        }
                                        :: serializationSet.Updates
                                },
                                fullUpdateHistograms
                        | None -> serializationSet, fullUpdateHistograms
                    | None -> serializationSet, fullUpdateHistograms

                let serializationSet, fullUpdateHistograms =
                    categorizeDirectedUpdateSerialization
                        directionAUpdates
                        serializationSet
                        fullUpdateHistograms

                let serializationSet, fullUpdateHistograms =
                    categorizeDirectedUpdateSerialization
                        directionBUpdates
                        serializationSet
                        fullUpdateHistograms

                readChannelDelta tail serializationSet fullUpdateHistograms
            | [] -> serializationSet, fullUpdateHistograms

        let serializationSet, fullUpdateHistograms =
            readChannelDelta
                (deltaSet |> Map.toList)
                serializationSet
                FullUpdateValueHistograms.Default

        { serializationSet with
            FullUpdateDefaults =
                {
                    CLTVExpiryDelta =
                        Histogram.findMostCommonHistogramEntryWithDefault
                            fullUpdateHistograms.CLTVExpiryDelta
                            BlockHeightOffset16.Zero
                    HTLCMinimumMSat =
                        Histogram.findMostCommonHistogramEntryWithDefault
                            fullUpdateHistograms.HTLCMinimumMSat
                            LNMoney.Zero
                    FeeBaseMSat =
                        Histogram.findMostCommonHistogramEntryWithDefault
                            fullUpdateHistograms.FeeBaseMSat
                            LNMoney.Zero
                    FeeProportionalMillionths =
                        Histogram.findMostCommonHistogramEntryWithDefault
                            fullUpdateHistograms.FeeProportionalMillionths
                            0u
                    HTLCMaximumMSat =
                        Histogram.findMostCommonHistogramEntryWithDefault
                            fullUpdateHistograms.HTLCMaximumMSat
                            LNMoney.Zero
                }
        }

    let serializeStrippedChannelAnnouncement
        (announcement: UnsignedChannelAnnouncementMsg)
        (nodeIdAIndex: int)
        (nodeIdBIndex: int)
        (previousShortChannelId: ShortChannelId)
        =
        if previousShortChannelId > announcement.ShortChannelId then
            failwith
                "Channels need to be sorted by ID before serialization can happen."

        use memStream = new MemoryStream()
        use writerStream = new LightningWriterStream(memStream)

        let features = announcement.FeatureBitsArray

        writerStream.WriteWithLen features

        writerStream.WriteBigSize(
            announcement.ShortChannelId.ToUInt64()
            - previousShortChannelId.ToUInt64()
        )

        writerStream.WriteBigSize(uint64 nodeIdAIndex)
        writerStream.WriteBigSize(uint64 nodeIdBIndex)

        memStream.ToArray()

    let serializeStrippedChannelUpdate
        (update: UpdateSerialization)
        (defaultValues: DefaultUpdateValues)
        (previousShortChannelId: ShortChannelId)
        =
        let latestUpdate = update.Update
        let channelFlags = latestUpdate.ChannelFlags

        if previousShortChannelId > latestUpdate.ShortChannelId then
            failwith
                "Channels need to be sorted by ID before serialization can happen."

        use deltaMemStream = new MemoryStream()
        use deltaWriterStream = new LightningWriterStream(deltaMemStream)

        let serializedFlags =
            match update.Mechanism with
            | UpdateSerializationMechanism.Full ->
                let serializedFlags = channelFlags

                let serializedFlags =
                    if latestUpdate.CLTVExpiryDelta
                       <> defaultValues.CLTVExpiryDelta then
                        deltaWriterStream.Write(
                            latestUpdate.CLTVExpiryDelta.Value,
                            false
                        )

                        serializedFlags ||| 0b0100_0000uy
                    else
                        serializedFlags

                let serializedFlags =
                    if latestUpdate.HTLCMinimumMSat
                       <> defaultValues.HTLCMinimumMSat then
                        deltaWriterStream.Write(
                            latestUpdate.HTLCMinimumMSat.MilliSatoshi |> uint64,
                            false
                        )

                        serializedFlags ||| 0b0010_0000uy
                    else
                        serializedFlags

                let serializedFlags =
                    if latestUpdate.FeeBaseMSat <> defaultValues.FeeBaseMSat then
                        deltaWriterStream.Write(
                            latestUpdate.FeeBaseMSat.MilliSatoshi |> uint32,
                            false
                        )

                        serializedFlags ||| 0b0001_0000uy
                    else
                        serializedFlags

                let serializedFlags =
                    if latestUpdate.FeeProportionalMillionths
                       <> defaultValues.FeeProportionalMillionths then
                        deltaWriterStream.Write(
                            latestUpdate.FeeProportionalMillionths,
                            false
                        )

                        serializedFlags ||| 0b0000_1000uy
                    else
                        serializedFlags

                let serializedFlags =
                    if latestUpdate.HTLCMaximumMSat.Value
                       <> defaultValues.HTLCMaximumMSat then
                        deltaWriterStream.Write(
                            latestUpdate.HTLCMaximumMSat.Value.MilliSatoshi
                            |> uint64,
                            false
                        )

                        serializedFlags ||| 0b0000_0100uy
                    else
                        serializedFlags

                serializedFlags
            | UpdateSerializationMechanism.Incremental mutatedProperties ->
                let serializedFlags = channelFlags ||| 0b1000_0000uy

                let serializedFlags =
                    if mutatedProperties.CltvExpiryDelta then
                        deltaWriterStream.Write(
                            latestUpdate.CLTVExpiryDelta.Value,
                            false
                        )

                        serializedFlags ||| 0b0100_0000uy
                    else
                        serializedFlags

                let serializedFlags =
                    if mutatedProperties.HtlcMinimumMSat then
                        deltaWriterStream.Write(
                            latestUpdate.HTLCMinimumMSat.MilliSatoshi |> uint64,
                            false
                        )

                        serializedFlags ||| 0b0010_0000uy
                    else
                        serializedFlags

                let serializedFlags =
                    if mutatedProperties.FeeBaseMSat then
                        deltaWriterStream.Write(
                            latestUpdate.FeeBaseMSat.MilliSatoshi |> uint32,
                            false
                        )

                        serializedFlags ||| 0b0001_0000uy
                    else
                        serializedFlags

                let serializedFlags =
                    if mutatedProperties.FeeProportionalMillionths then
                        deltaWriterStream.Write(
                            latestUpdate.FeeProportionalMillionths,
                            false
                        )

                        serializedFlags ||| 0b0000_1000uy
                    else
                        serializedFlags

                let serializedFlags =
                    if mutatedProperties.HtlcMaximumMSat then
                        deltaWriterStream.Write(
                            latestUpdate.HTLCMaximumMSat.Value.MilliSatoshi
                            |> uint64,
                            false
                        )

                        serializedFlags ||| 0b0000_0100uy
                    else
                        serializedFlags

                serializedFlags

        use prefixedMemStream = new MemoryStream()
        use prefixedWriterStream = new LightningWriterStream(prefixedMemStream)

        prefixedWriterStream.WriteBigSize(
            update.Update.ShortChannelId.ToUInt64()
            - previousShortChannelId.ToUInt64()
        )

        prefixedWriterStream.WriteByte serializedFlags
        prefixedWriterStream.Write(deltaMemStream.ToArray())

        prefixedMemStream.ToArray()

    member __.SerializeDelta
        (networkGraph: NetworkGraph)
        (lastSyncTimestamp: DateTime)
        (considerIntermediateUpdates: bool)
        =
        async {
            use outputMemStream = new MemoryStream()
            use outputWriter = new LightningWriterStream(outputMemStream)

            let nodeIds = ResizeArray<PubKey>()
            let mutable nodeIdsIndices = Map<array<byte>, int> Seq.empty

            let getNodeIdIndex(nodeId: NodeId) =
                let serializedNodeId = nodeId.Value.ToBytes()

                if nodeIdsIndices |> Map.containsKey serializedNodeId |> not then
                    nodeIds.Add nodeId.Value
                    let index = nodeIds.Count - 1

                    nodeIdsIndices <-
                        Map.add serializedNodeId index nodeIdsIndices

                    index
                else
                    nodeIdsIndices.[serializedNodeId]

            let deltaSet = DeltaSet Seq.empty

            let! deltaSet =
                fetchChannelAnnouncements
                    deltaSet
                    networkGraph
                    lastSyncTimestamp

            let! deltaSet =
                fetchChannelUpdates
                    deltaSet
                    lastSyncTimestamp
                    considerIntermediateUpdates

            let deltaSet = filterDeltaSet deltaSet

            let serializationDetails =
                serializeDeltaSet
                    deltaSet
                    (DateTimeUtils.ToUnixTimestamp lastSyncTimestamp)

            let announcementCount =
                uint32 serializationDetails.Announcements.Length

            outputWriter.Write(announcementCount, false)

            let announcements =
                serializationDetails.Announcements
                |> List.sortBy(fun ann -> ann.ShortChannelId.ToUInt64())

            // process announcements
            // write the number of channel announcements to the output
            let rec writeAnnouncements
                (announcements: List<UnsignedChannelAnnouncementMsg>)
                (previousShortChannelId: ShortChannelId)
                =
                match announcements with
                | currentAnnouncement :: tail ->
                    let idIndex1 = getNodeIdIndex currentAnnouncement.NodeId1
                    let idIndex2 = getNodeIdIndex currentAnnouncement.NodeId2

                    let strippedAnnouncement =
                        serializeStrippedChannelAnnouncement
                            currentAnnouncement
                            idIndex1
                            idIndex2
                            previousShortChannelId

                    outputMemStream.Write(
                        strippedAnnouncement,
                        0,
                        strippedAnnouncement.Length
                    )

                    writeAnnouncements tail currentAnnouncement.ShortChannelId
                | [] -> ()

            writeAnnouncements announcements (ShortChannelId.FromUInt64 0UL)

            let updateCount = uint32 serializationDetails.Updates.Length
            outputWriter.Write(updateCount, false)

            let defaultValues = serializationDetails.FullUpdateDefaults

            if updateCount > 0u then
                outputWriter.Write(defaultValues.CLTVExpiryDelta.Value, false)

                outputWriter.Write(
                    defaultValues.HTLCMinimumMSat.Value |> uint64,
                    false
                )

                outputWriter.Write(
                    defaultValues.FeeBaseMSat.MilliSatoshi |> uint32,
                    false
                )

                outputWriter.Write(
                    defaultValues.FeeProportionalMillionths,
                    false
                )

                outputWriter.Write(
                    defaultValues.HTLCMaximumMSat.MilliSatoshi |> uint64,
                    false
                )

            let updates =
                serializationDetails.Updates
                |> List.sortBy(fun update ->
                    update.Update.ShortChannelId.ToUInt64()
                )

            let rec writeUpdates
                (updates: List<UpdateSerialization>)
                (previousShortChannelId: ShortChannelId)
                =
                match updates with
                | currentUpdate :: tail ->
                    let strippedChannelUpdate =
                        serializeStrippedChannelUpdate
                            currentUpdate
                            defaultValues
                            previousShortChannelId

                    outputMemStream.Write(
                        strippedChannelUpdate,
                        0,
                        strippedChannelUpdate.Length
                    )

                    writeUpdates tail currentUpdate.Update.ShortChannelId
                | [] -> ()

            writeUpdates updates (ShortChannelId.FromUInt64 0UL)

            let prefixedOutputMemStream = new MemoryStream()

            let prefixedOutputWriter =
                new LightningWriterStream(prefixedOutputMemStream)

            let prefix = [| 76uy; 68uy; 75uy; 1uy |]
            prefixedOutputWriter.Write prefix
            prefixedOutputWriter.Write(serializationDetails.ChainHash, true)
            let lastSeenTimestamp = serializationDetails.LatestSeen

            let overflowSeconds =
                let dayInSeconds = TimeSpan.FromDays(1.).TotalSeconds |> uint32
                lastSeenTimestamp % dayInSeconds

            prefixedOutputWriter.Write(
                lastSeenTimestamp - overflowSeconds,
                false
            )

            let nodeIdCount = nodeIds.Count |> uint32
            prefixedOutputWriter.Write(nodeIdCount, false)

            for currentNodeId in nodeIds do
                currentNodeId.ToBytes() |> prefixedOutputWriter.Write

            outputMemStream.ToArray() |> prefixedOutputWriter.Write

            let messageCount = announcementCount + updateCount

            Logger.Log
                "GossipSnapshotter"
                (sprintf "Snapshot created! Message count: %i" messageCount)

            return prefixedOutputMemStream.ToArray()
        }

    member self.Start() =
        async {
            do! startToken.WaitHandle |> Async.AwaitWaitHandle |> Async.Ignore

            let rec snapshot() =
                async {
                    try
                        let snapshotGenerationTimestamp = DateTime.UtcNow

                        let snapshotSyncDayFactors =
                            [
                                1
                                2
                                3
                                4
                                5
                                6
                                7
                                14
                                21
                                Int32.MaxValue
                            ]

                        let referenceTimestamp =
                            snapshotGenerationTimestamp.Date

                        Logger.Log
                            "GossipSnapshotter"
                            (sprintf
                                "Capturing snapshots at %i for: %i"
                                (DateTimeUtils.ToUnixTimestamp
                                    snapshotGenerationTimestamp)
                                (DateTimeUtils.ToUnixTimestamp
                                    referenceTimestamp))

                        let snapshotSyncTimestamps =
                            snapshotSyncDayFactors
                            |> List.map(fun factor ->
                                let timestamp =
                                    if factor <> Int32.MaxValue then
                                        referenceTimestamp.Subtract(
                                            factor |> float |> TimeSpan.FromDays
                                        )
                                    else
                                        DateTime.MinValue

                                factor, timestamp
                            )

                        use batch = dataSource.CreateBatch()

                        batch.BatchCommands.Add(
                            batch.CreateBatchCommand(
                                CommandText = "DELETE FROM snapshots"
                            )
                        )

                        //WARNING: don't try to parallelize this, it chews memory.
                        for (dayRange, currentLastSyncTimestamp) in
                            snapshotSyncTimestamps do
                            let stopWatch = Stopwatch()

                            stopWatch.Start()

                            let! snapshot =
                                self.SerializeDelta
                                    networkGraph
                                    currentLastSyncTimestamp
                                    true

                            stopWatch.Stop()

                            Logger.Log
                                "GossipSnapshotter"
                                (sprintf
                                    "Snapshot creation took %f seconds to create it"
                                    stopWatch.Elapsed.TotalSeconds)

                            let batchCommand =
                                batch.CreateBatchCommand(
                                    CommandText =
                                        "INSERT INTO snapshots(\"referenceDateTime\", \"blob\", \"dayRange\", \"lastSyncTimestamp\") VALUES ($1,$2,$3,$4)"
                                )

                            batchCommand.Parameters.Add(
                                NpgsqlParameter(Value = referenceTimestamp)
                            )
                            |> ignore

                            batchCommand.Parameters.Add(
                                NpgsqlParameter(Value = snapshot)
                            )
                            |> ignore

                            batchCommand.Parameters.Add(
                                NpgsqlParameter(Value = dayRange)
                            )
                            |> ignore

                            batchCommand.Parameters.Add(
                                NpgsqlParameter(
                                    Value = currentLastSyncTimestamp
                                )
                            )
                            |> ignore

                            batch.BatchCommands.Add batchCommand

                        do!
                            batch.ExecuteNonQueryAsync()
                            |> Async.AwaitTask
                            |> Async.Ignore

                        // constructing the snapshots may have taken a while
                        let nextSnapshot =
                            DateTime.UtcNow.Date.AddDays(1.).AddMinutes(1.)

                        let timeUntilNextDay =
                            nextSnapshot.Subtract DateTime.UtcNow

                        Logger.Log
                            "GossipSnapshotter"
                            (sprintf
                                "Sleeping until next snapshot capture: %fs"
                                timeUntilNextDay.TotalSeconds)

                        // sleep until next day
                        do! Async.Sleep timeUntilNextDay
                        return! snapshot()
                    with
                    | ex -> Logger.Log "GossipSnapshotter" (ex.ToString())
                }

            do! snapshot()
        }
