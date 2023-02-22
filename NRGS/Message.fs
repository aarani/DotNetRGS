namespace NRGS

open DotNetLightning.Serialization.Msgs

type Message =
    | RoutingMsg of msg: IRoutingMsg * bytes: array<byte>
    | FinishedInitialSync
