namespace NRGS

open DotNetLightning.Serialization.Msgs
open NBitcoin

type Message =
    | RoutingMsg of msg: IRoutingMsg * bytes: array<byte>
    | VerifiedChannelAnnouncement of
        channelAnn: ChannelAnnouncementMsg *
        capacity: Option<Money> *
        bytes: array<byte>
    | FinishedInitialSync
