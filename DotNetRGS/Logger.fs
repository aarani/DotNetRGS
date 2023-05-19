namespace DotNetRGS

open System

module Logger =
    let Log (area: string) (text: string) =
        Console.WriteLine(
            sprintf "%s[%s]: %s" area (DateTime.UtcNow.ToString()) text
        )
