namespace DotNetRGS

open System
open System.Threading
open System.IO

open DotNetLightning.Utils
open DotNetLightning.Serialization.Msgs
open NBitcoin
open Newtonsoft.Json
open GWallet.Backend

open DotNetRGS.Utils

type ChannelInfo =
    {
        NodeOne: NodeId
        NodeTwo: NodeId
        Forward: Option<UnsignedChannelUpdateMsg>
        Backward: Option<UnsignedChannelUpdateMsg>
        AnnouncementReceivedTime: DateTime
        Capacity: Option<Money>
    }

type internal ShortChannelIdConverter() =
    inherit JsonConverter<ShortChannelId>()

    override __.ReadJson
        (
            reader: JsonReader,
            _: Type,
            _: ShortChannelId,
            _: bool,
            serializer: JsonSerializer
        ) =
        let serializedChannelId = serializer.Deserialize<UInt64> reader
        serializedChannelId |> ShortChannelId.FromUInt64

    override __.WriteJson
        (
            writer: JsonWriter,
            state: ShortChannelId,
            serializer: JsonSerializer
        ) =
        serializer.Serialize(writer, state.ToUInt64())

type internal NodeIdConverter() =
    inherit JsonConverter<NodeId>()

    override __.ReadJson
        (
            reader: JsonReader,
            _: Type,
            _: NodeId,
            _: bool,
            serializer: JsonSerializer
        ) =
        let serializedChannelId = serializer.Deserialize<string> reader
        serializedChannelId |> NBitcoin.PubKey |> NodeId

    override __.WriteJson
        (
            writer: JsonWriter,
            state: NodeId,
            serializer: JsonSerializer
        ) =
        serializer.Serialize(writer, state.Value.ToHex())

type NetworkGraph(dataDir: DirectoryInfo) =
    let serializationSettings =
        let serializationSettings =
            GWallet.Backend.UtxoCoin.Lightning.SerializedChannel.LightningSerializerSettings
                Currency.BTC

        serializationSettings.Converters.Add(ShortChannelIdConverter())
        serializationSettings.Converters.Add(NodeIdConverter())
        serializationSettings


    let mutable channels: Map<ShortChannelId, ChannelInfo> = Map.empty
    let channelsLock = obj()

    do
        if not dataDir.Exists then
            dataDir.Create()

        let channelsFile = Path.Combine(dataDir.FullName, "channels.json")

        if File.Exists channelsFile then
            let channelsJson = File.ReadAllText channelsFile

            channels <-
                JsonConvert.DeserializeObject<List<ShortChannelId * ChannelInfo>>(
                    channelsJson,
                    serializationSettings
                )
                |> Map.ofList

    new() =
        let configPath =
            Environment.GetFolderPath Environment.SpecialFolder.ApplicationData

        let path = Path.Combine(configPath, "DotNetRGS") |> DirectoryInfo
        NetworkGraph path

    member __.ValidateChannelAnnouncement(ann: UnsignedChannelAnnouncementMsg) =
        Monitor.Enter channelsLock

        try
            match channels.TryGetValue ann.ShortChannelId with
            | true, channel ->
                if channel.NodeOne = ann.NodeId1
                   && channel.NodeTwo = ann.NodeId2 then
                    false
                else
                    true
            | false, _ -> true
        finally
            Monitor.Exit channelsLock

    member __.AddChannel
        (ann: UnsignedChannelAnnouncementMsg)
        (capacityOpt: Option<Money>)
        =
        Monitor.Enter channelsLock

        try
            let newChannelInfo =
                {
                    ChannelInfo.NodeOne = ann.NodeId1
                    NodeTwo = ann.NodeId2
                    Forward = None
                    Backward = None
                    AnnouncementReceivedTime = DateTime.UtcNow
                    Capacity = capacityOpt
                }

            match channels.TryGetValue ann.ShortChannelId with
            | true, channel ->
                if channel.NodeOne = ann.NodeId1
                   && channel.NodeTwo = ann.NodeId2 then
                    ()
                else
                    channels <- channels.Add(ann.ShortChannelId, newChannelInfo)
            | false, _ ->
                channels <- channels.Add(ann.ShortChannelId, newChannelInfo)
        finally
            Monitor.Exit channelsLock

    member __.AddChannelUpdate(signedUpdateMsg: ChannelUpdateMsg) =
        let unsignedUpdateMsg = signedUpdateMsg.Contents

        let updateMsgHash =
            unsignedUpdateMsg.ToBytes() |> NBitcoin.Crypto.Hashes.DoubleSHA256

        let now = DateTimeUtils.ToUnixTimestamp DateTime.UtcNow
        let twoWeekInSeconds = TimeSpan.FromDays(14.).TotalSeconds |> uint32
        let dayInSeconds = TimeSpan.FromDays(1.).TotalSeconds |> uint32

        if unsignedUpdateMsg.Timestamp < now - twoWeekInSeconds then
            Logger.Log
                "NetworkGraph"
                "AddChannelUpdate: received channel update older than two weeks"

            false
        elif unsignedUpdateMsg.Timestamp > now + dayInSeconds then
            Logger.Log
                "NetworkGraph"
                "AddChannelUpdate: channel_update has a timestamp more than a day in the future"

            false
        else
            Monitor.Enter channelsLock

            try
                match channels.TryGetValue unsignedUpdateMsg.ShortChannelId with
                | true, channel ->
                    // This code checks that if we have the capacity (we're in release mode and
                    // we check the channel on-chain), it shouldn't be less than HtLCMaximumMSat.
                    match (channel.Capacity, unsignedUpdateMsg.HTLCMaximumMSat)
                        with
                    | Some capacity, Some htlcMaximum when
                        capacity.Satoshi < htlcMaximum.Satoshi
                        ->
                        Logger.Log
                            "NetworkGraph"
                            "AddChannelUpdate: received channel update with htlc maximum more than the capacity"

                        false
                    // While DNL allows for HTLCMaximumMSat being None most implementations reject updates
                    // with no HTLCMaximumMSat, our logic depend on HTLCMaximumMSat's value so we reject them as well.
                    // https://github.com/ACINQ/eclair/commit/c71c3b40465a6fadc8a5cca982a5b466fd0aedfc
                    // https://github.com/lightning/bolts/commit/6fee63fc342736a2eb7f6e856ae0d85807cc50ec
                    | _, None ->
                        Logger.Log
                            "NetworkGraph"
                            "AddChannelUpdate: received channel update with no htlc maximum"

                        false
                    | _ ->
                        let isForward =
                            (unsignedUpdateMsg.ChannelFlags &&& 1uy) = 0uy

                        let getNewer
                            (previousValueOpt: Option<UnsignedChannelUpdateMsg>)
                            (newValue: UnsignedChannelUpdateMsg)
                            =
                            match previousValueOpt with
                            | Some previousValue ->
                                if previousValue.Timestamp >= newValue.Timestamp then
                                    Logger.Log
                                        "NetworkGraph"
                                        "AddChannelUpdate: received update older or same as the last processed update"

                                    previousValue
                                else
                                    newValue
                            | None -> newValue

                        // We have to do the signature verification here instead of GossipVerifier because we need the channel nodeIds
                        let sigIsValid =
                            if isForward then
                                channel.NodeOne.Value.Verify(
                                    updateMsgHash,
                                    signedUpdateMsg.Signature.Value
                                )
                            else
                                channel.NodeTwo.Value.Verify(
                                    updateMsgHash,
                                    signedUpdateMsg.Signature.Value
                                )

                        if sigIsValid then
                            let newChannel =
                                if isForward then
                                    { channel with
                                        Forward =
                                            getNewer
                                                channel.Forward
                                                unsignedUpdateMsg
                                            |> Some
                                    }
                                else
                                    { channel with
                                        Backward =
                                            getNewer
                                                channel.Backward
                                                unsignedUpdateMsg
                                            |> Some
                                    }

                            channels <-
                                channels.Add(
                                    unsignedUpdateMsg.ShortChannelId,
                                    newChannel
                                )

                            channel <> newChannel
                        else
                            Logger.Log
                                "NetworkGraph"
                                "AddChannelUpdate: received updateMsg with invalid signature"

                            false
                | false, _ ->
                    Logger.Log
                        "NetworkGraph"
                        "AddChannelUpdate: received channel update for unknown channel"

                    false
            finally
                Monitor.Exit channelsLock

    member private __.UnsafeRemoveStaleChannels() =
        //We remove stale channel directional info two weeks after the last update, per BOLT 7's suggestion.
        let minUpdateDateTime = DateTime.UtcNow.Subtract(TimeSpan.FromDays 14.)
        let minUpdateTimestamp = DateTimeUtils.ToUnixTimestamp minUpdateDateTime

        channels <-
            channels
            |> Map.toSeq
            |> Seq.choose(fun (shortChannelId, info) ->
                let info =
                    if info.Forward.IsSome
                       && info.Forward.Value.Timestamp < minUpdateTimestamp then
                        { info with
                            Forward = None
                        }
                    else
                        info

                let info =
                    if info.Backward.IsSome
                       && info.Backward.Value.Timestamp < minUpdateTimestamp then
                        { info with
                            Backward = None
                        }
                    else
                        info

                if info.Forward.IsNone && info.Backward.IsNone then
                    // We check the announcement_received_time here to ensure we don't drop
                    // announcements that we just received and are just waiting for our peer to send a
                    // channel_update for.
                    if info.AnnouncementReceivedTime < minUpdateDateTime then
                        None
                    else
                        Some(shortChannelId, info)
                else
                    Some(shortChannelId, info)
            )
            |> Map.ofSeq

    member _.GetChannelIds() =
        Monitor.Enter channelsLock

        try
            channels.Keys |> Array.ofSeq
        finally
            Monitor.Exit channelsLock

    member self.Save() =
        Monitor.Enter channelsLock

        try
            self.UnsafeRemoveStaleChannels()
            let channelsFile = Path.Combine(dataDir.FullName, "channels.json")
            let channelsList = channels |> Map.toList

            let channelsJson =
                JsonConvert.SerializeObject(channelsList, serializationSettings)

            File.WriteAllText(channelsFile, channelsJson)
        finally
            Monitor.Exit channelsLock
