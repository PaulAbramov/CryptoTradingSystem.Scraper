using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using CryptoTradingSystem.General.Data;
using Microsoft.Extensions.Configuration;

namespace CryptoTradingSystem.Scraper
{
    class Program
    {
        static void Main(string[] args)
        {
            IConfiguration config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();

            var connectionString = config.GetValue<string>("ConnectionString");

            DatabaseHandler.InitializeDatabase(connectionString);

            CultureInfo info = new CultureInfo("de-DE")
            {
                NumberFormat =
                {
                    NumberDecimalSeparator = "."
                }
            };

            Thread.CurrentThread.CurrentCulture = info;
            Thread.CurrentThread.CurrentUICulture = Thread.CurrentThread.CurrentCulture;

            var tasks = new Dictionary<Enums.Assets, Dictionary<Enums.TimeFrames, Task>>();

            Task scrapperTask = Task.Factory.StartNew(() =>
            {
                var scrapper = new Scrapper();
                scrapper.StartScrapping(connectionString);
            });


            foreach (var asset in (Enums.Assets[])Enum.GetValues(typeof(Enums.Assets)))
            {
                tasks.Add(asset, new Dictionary<Enums.TimeFrames, Task>());

                foreach (var timeFrame in (Enums.TimeFrames[])Enum.GetValues(typeof(Enums.TimeFrames)))
                {
                    tasks[asset].Add(timeFrame, CreateWebsocket(asset, timeFrame, connectionString));
                }
            }

            bool runScrapper = false;
            bool allowToRunScrapper = false;

            while (true)
            {
                if (scrapperTask.Status != TaskStatus.Running &&
                    scrapperTask.Status != TaskStatus.WaitingToRun)
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
                        scrapperTask = Task.Factory.StartNew(() =>
                        {
                            var scrapper = new Scrapper();
                            scrapper.StartScrapping(connectionString);
                        });

                        runScrapper = false;
                    }
                }

                foreach (var asset in (Enums.Assets[])Enum.GetValues(typeof(Enums.Assets)))
                {
                    foreach (var timeFrame in (Enums.TimeFrames[])Enum.GetValues(typeof(Enums.TimeFrames)))
                    {
                        var taskToCheck = tasks[asset][timeFrame];
                        if (taskToCheck.Status != TaskStatus.Running &&
                            taskToCheck.Status != TaskStatus.WaitingToRun)
                        {
                            tasks[asset][timeFrame] = CreateWebsocket(asset, timeFrame, connectionString);
                        }
                    }
                }

                Task.Delay(500).GetAwaiter().GetResult();
            }
        }

        /// <summary>
        /// Get the Data from Binance and open up a websocket to the asset and timeframe income stream
        /// </summary>
        /// <param name="_asset"></param>
        /// <param name="_timeFrame"></param>
        /// <param name="_connectionString"></param>
        /// <returns></returns>
        private static Task CreateWebsocket(Enums.Assets _asset, Enums.TimeFrames _timeFrame, string _connectionString)
        {
            var taskToWork = Task.Factory.StartNew(() =>
            {
                WebSocketManager manager = new WebSocketManager();

                var quotes = ApiManager.GetBinanceData(_asset, _timeFrame).GetAwaiter().GetResult();
                if (quotes.Count > 0)
                {
                    manager.CreateWebSocket(_asset, _timeFrame, _connectionString);
                }
                else
                {
                    Console.WriteLine($"{_asset} | {_timeFrame} | getting quotes did not work");
                }
            });

            return taskToWork;
        }
    }
}
