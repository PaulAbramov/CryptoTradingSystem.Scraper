using CryptoTradingSystem.General.Data;
using CryptoTradingSystem.Scraper.Binance;
using CryptoTradingSystem.Scraper.Bybit;
using Microsoft.Extensions.Configuration;
using Serilog;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace CryptoTradingSystem.Scraper;

internal static class Program
{
	private static void Main(string[] args)
	{
		IConfiguration config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();

		var loggingfilePath = config.GetValue<string>("LoggingLocation");
		Log.Logger = new LoggerConfiguration()
			.MinimumLevel.Debug()
#if RELEASE
                .WriteTo.Console(restrictedToMinimumLevel: Serilog.Events.LogEventLevel.Information,
                                outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
#endif
#if DEBUG
			.WriteTo.Console(
				outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
#endif
			.WriteTo.File(
				loggingfilePath,
				rollingInterval: RollingInterval.Day,
				outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
			.CreateLogger();

		var connectionString = config.GetValue<string>("ConnectionString");
		DatabaseHandler.InitializeDatabase(connectionString);

		var info = new CultureInfo("de-DE")
		{
			NumberFormat =
			{
				NumberDecimalSeparator = "."
			}
		};

		Thread.CurrentThread.CurrentCulture = info;
		Thread.CurrentThread.CurrentUICulture = Thread.CurrentThread.CurrentCulture;

		var tasks = new Dictionary<Enums.Exchange, Dictionary<Enums.Assets, Dictionary<Enums.TimeFrames, Task>>>();

		var scrapperTask = Task.Factory.StartNew(
			async () =>
			{
				foreach (var exchange in (Enums.Exchange[]) Enum.GetValues(typeof(Enums.Exchange)))
				{
					switch (exchange)
					{
						case Enums.Exchange.Binance:
							var binanceScraper = new BinanceScraper();
							await binanceScraper.StartScrapping(connectionString);
							break;
						case Enums.Exchange.Bybit:
							var bybitScraper = new BybitScraper();
							await bybitScraper.StartScrapping(connectionString);
							break;
						default:
							throw new ArgumentOutOfRangeException(nameof(exchange), exchange, null);
					}
				}
			});

		foreach (var exchange in (Enums.Exchange[]) Enum.GetValues(typeof(Enums.Exchange)))
		{
			tasks.Add(exchange, new());

			foreach (var asset in (Enums.Assets[]) Enum.GetValues(typeof(Enums.Assets)))
			{
				tasks[exchange].Add(asset, new());

				foreach (var timeFrame in (Enums.TimeFrames[]) Enum.GetValues(typeof(Enums.TimeFrames)))
				{
					tasks[exchange][asset].Add(timeFrame, CreateWebsocket(exchange, asset, timeFrame, connectionString));
				}
			}
		}

		var runScrapper = false;
		var allowToRunScrapper = false;

		while (true)
		{
			HandleScrapperStatus(scrapperTask, runScrapper, allowToRunScrapper, connectionString, tasks);

			Task.Delay(500).GetAwaiter().GetResult();
		}
	}

	private static void HandleScrapperStatus(
		Task scrapperTask,
		bool runScrapper,
		bool allowToRunScrapper,
		string connectionString,
		Dictionary<Enums.Exchange, Dictionary<Enums.Assets, Dictionary<Enums.TimeFrames, Task>>> tasks)
	{
		if (scrapperTask.Status != TaskStatus.Running && scrapperTask.Status != TaskStatus.WaitingToRun)
		{
			RestartScrapperOnEverySecondDayOfMonth(scrapperTask, runScrapper, allowToRunScrapper, connectionString);
		}
	}

	private static void RestartScrapperOnEverySecondDayOfMonth(
		Task scrapperTask,
		bool runScrapper,
		bool allowToRunScrapper,
		string connectionString)
	{
		if (DateTime.Now.Day == 2)
		{
			if (allowToRunScrapper)
			{
				allowToRunScrapper = false;
				runScrapper = true;
			}
		}
		else
		{
			allowToRunScrapper = true;
		}

		if (runScrapper)
		{
			scrapperTask = Task.Factory.StartNew(
				async () =>
				{
					foreach (var exchange in (Enums.Exchange[]) Enum.GetValues(typeof(Enums.Exchange)))
					{
						switch (exchange)
						{
							case Enums.Exchange.Binance:
								var binanceScraper = new BinanceScraper();
								await binanceScraper.StartScrapping(connectionString);
								break;
							case Enums.Exchange.Bybit:
								var bybitScraper = new BybitScraper();
								await bybitScraper.StartScrapping(connectionString);
								break;
							default:
								throw new ArgumentOutOfRangeException(nameof(exchange), exchange, null);
						}
					}
				});

			runScrapper = false;
		}
	}

	/// <summary>
	///   Open up a websocket for each exchange, asset and timeframe
	/// </summary>
	/// <param name="exchange"></param>
	/// <param name="asset"></param>
	/// <param name="timeFrame"></param>
	/// <param name="connectionString"></param>
	/// <returns></returns>
	private static async Task<Task> CreateWebsocket(
		Enums.Exchange exchange,
		Enums.Assets asset,
		Enums.TimeFrames timeFrame,
		string connectionString)
	{
		var taskToWork = await Task.Factory.StartNew(
			async Task () =>
			{
				switch (exchange)
				{
					case Enums.Exchange.Binance:
						var binanceWebSocket = new BinanceWebSocket();
						await binanceWebSocket.CreateWebSocket(asset, timeFrame, connectionString);
						break;
					case Enums.Exchange.Bybit:
						var bybitWebSocket = new BybitWebSocket();
						await bybitWebSocket.CreateWebSocket(asset, timeFrame, connectionString);
						break;
					default:
						throw new ArgumentOutOfRangeException(nameof(exchange), exchange, null);
				}
			});

		return taskToWork;
	}
}