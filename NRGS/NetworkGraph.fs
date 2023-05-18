namespace NRGS

open System
open System.Threading
open System.IO

open DotNetLightning.Utils
open DotNetLightning.Serialization.Msgs
open Newtonsoft.Json
open GWallet.Backend

open NRGS.Utils

type ChannelInfo =
    {
        NodeOne: NodeId
        NodeTwo: NodeId
        Forward: Option<UnsignedChannelUpdateMsg>
        Backward: Option<UnsignedChannelUpdateMsg>
        AnnouncementReceivedTime: DateTime
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

        let path = Path.Combine(configPath, "nrgs") |> DirectoryInfo
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

    member __.AddChannel(ann: UnsignedChannelAnnouncementMsg) =
        Monitor.Enter channelsLock

        try
            let newChannelInfo =
                {
                    ChannelInfo.NodeOne = ann.NodeId1
                    NodeTwo = ann.NodeId2
                    Forward = None
                    Backward = None
                    AnnouncementReceivedTime = DateTime.UtcNow
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

    member __.AddChannelUpdate(updateMsg: UnsignedChannelUpdateMsg) =
        let now = DateTimeUtils.ToUnixTimestamp DateTime.UtcNow
        let twoWeekInSeconds = TimeSpan.FromDays(14.).TotalSeconds |> uint32
        let dayInSeconds = TimeSpan.FromDays(1.).TotalSeconds |> uint32

        if updateMsg.Timestamp < now - twoWeekInSeconds then
            Console.WriteLine
                "AddChannelUpdate: received channel update older than two weeks"

            false
        elif updateMsg.Timestamp > now + dayInSeconds then
            Console.WriteLine
                "AddChannelUpdate: channel_update has a timestamp more than a day in the future"

            false
        else
            Monitor.Enter channelsLock

            try
                match channels.TryGetValue updateMsg.ShortChannelId with
                | true, channel ->
                    let isForward = (updateMsg.ChannelFlags &&& 1uy) = 0uy

                    let getNewer
                        (previousValueOpt: Option<UnsignedChannelUpdateMsg>)
                        (newValue: UnsignedChannelUpdateMsg)
                        =
                        match previousValueOpt with
                        | Some previousValue ->
                            if previousValue.Timestamp >= newValue.Timestamp then
                                Console.WriteLine(
                                    "AddChannelUpdate: Update older or same timestamp than last processed update"
                                )

                                previousValue
                            else
                                newValue
                        | None -> newValue

                    let newChannel =
                        if isForward then
                            { channel with
                                Forward =
                                    getNewer channel.Forward updateMsg |> Some
                            }
                        else
                            { channel with
                                Backward =
                                    getNewer channel.Backward updateMsg |> Some
                            }

                    channels <-
                        channels.Add(updateMsg.ShortChannelId, newChannel)

                    channel <> newChannel
                | false, _ ->
                    Console.WriteLine
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
