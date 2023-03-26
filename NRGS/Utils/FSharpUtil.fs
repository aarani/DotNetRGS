namespace NRGS.Utils

module FSharpUtil =
    let UnwrapOption<'T> (opt: Option<'T>) (msg: string) : 'T =
        match opt with
        | Some value -> value
        | None -> failwith <| sprintf "error unwrapping Option: %s" msg
