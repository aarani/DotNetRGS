namespace DotNetRGS.Utils

type Histogram<'T when 'T: comparison> = Map<'T, uint>

module Histogram =
    let empty = Map.empty

    let findMostCommonHistogramEntryWithDefault<'T when 'T: comparison>
        (histogram: Histogram<'T>)
        (defaultValue: 'T)
        =
        if histogram |> Map.isEmpty then
            defaultValue
        else
            let mostFrequentEntry =
                histogram
                |> Map.toSeq
                |> Seq.maxBy(fun (_value, count) -> count)

            let (value, _count) = mostFrequentEntry

            value
