module DotNetRGS.Server.App

open System
open System.IO
open Microsoft.AspNetCore.Builder
open Microsoft.Extensions.Configuration
open Microsoft.AspNetCore.Hosting
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.DependencyInjection
open Microsoft.AspNetCore.Http

open Giraffe
open Npgsql

open DotNetRGS.Server.Utils

let snapshotServer(lastSyncTimestamp: string) : HttpHandler =
    fun (_next: HttpFunc) (ctx: HttpContext) ->
        task {
            let dataSource = ctx.GetService<NpgsqlDataSource>()

            let lastSyncTimestamp =
                Convert.ToUInt32 lastSyncTimestamp
                |> DateTimeUtils.FromUnixTimestamp

            use command =
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

let configureServices
    (configuration: IConfiguration)
    (services: IServiceCollection)
    =
    services
        .AddGiraffe()
        .AddNpgsqlDataSource(configuration.GetConnectionString("MainDB"))
    |> ignore<IServiceCollection>

let configureLogging(builder: ILoggingBuilder) =
    builder.AddConsole().AddDebug() |> ignore

[<EntryPoint>]
let main args =
    let configuration =
        ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", false, false)
            .Build()

    Host
        .CreateDefaultBuilder(args)
        .ConfigureWebHostDefaults(fun webHostBuilder ->
            webHostBuilder
                .Configure(Action<IApplicationBuilder> configureApp)
                .ConfigureServices(configureServices configuration)
                .ConfigureLogging(configureLogging)
            |> ignore
        )
        .Build()
        .Run()

    0
