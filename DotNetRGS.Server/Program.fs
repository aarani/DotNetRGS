module DotNetRGS.Server.App

open System
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.DependencyInjection
open Microsoft.AspNetCore.Http

open Giraffe
open Npgsql

open DotNetRGS.Server.Utils

let dataSource =
    NpgsqlDataSource.Create
        "Host=127.0.0.1;Username=postgres;Password=f50d47dc6afe40918afa2a935637ec1e;Database=nrgs"

let snapshotServer(lastSyncTimestamp: string) : HttpHandler =
    fun (_next: HttpFunc) (ctx: HttpContext) ->
        task {
            let lastSyncTimestamp =
                Convert.ToUInt32 lastSyncTimestamp
                |> DateTimeUtils.FromUnixTimestamp

            let command =
                dataSource.CreateCommand(
                    commandText =
                        "SELECT \"blob\" FROM snapshots WHERE \"lastSyncTimestamp\" <= $1 ORDER BY \"lastSyncTimestamp\" DESC LIMIT 1"
                )

            command.Parameters.AddWithValue lastSyncTimestamp |> ignore
            use reader = command.ExecuteReader()

            if reader.HasRows then
                let readResult = reader.Read()

                if readResult then
                    let readStream = reader.GetStream 0
                    return! ctx.WriteStreamAsync(false, readStream, None, None)
                else
                    ctx.SetStatusCode 404
                    return! ctx.WriteTextAsync "Not found!"
            else
                ctx.SetStatusCode 404
                return! ctx.WriteTextAsync "Not found!"
        }

let webApp =
    choose
        [
            GET >=> choose [ routef "/snapshot/%s" snapshotServer ]
            setStatusCode 404 >=> text "Not Found"
        ]

let errorHandler (ex: Exception) (logger: ILogger) =
    logger.LogError(
        ex,
        "An unhandled exception has occurred while executing the request."
    )

    clearResponse >=> setStatusCode 500

let configureApp(app: IApplicationBuilder) =
    let env = app.ApplicationServices.GetService<IWebHostEnvironment>()

    (match env.IsDevelopment() with
     | true -> app.UseDeveloperExceptionPage()
     | false -> app.UseGiraffeErrorHandler errorHandler)
        .UseGiraffe webApp

let configureServices(services: IServiceCollection) =
    services.AddGiraffe() |> ignore

let configureLogging(builder: ILoggingBuilder) =
    builder.AddConsole().AddDebug() |> ignore

[<EntryPoint>]
let main args =
    Host
        .CreateDefaultBuilder(args)
        .ConfigureWebHostDefaults(fun webHostBuilder ->
            webHostBuilder
                .Configure(Action<IApplicationBuilder> configureApp)
                .ConfigureServices(configureServices)
                .ConfigureLogging(configureLogging)
            |> ignore
        )
        .Build()
        .Run()

    0
