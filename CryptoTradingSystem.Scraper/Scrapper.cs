using System;
using System.Collections.Generic;
using System.IO;
using CryptoTradingSystem.General.Data;
using CryptoTradingSystem.General.Database.Models;
using CryptoTradingSystem.General.Helper;
using Serilog;

namespace CryptoTradingSystem.Scraper
{
    internal class Scrapper
    {
        private const string baseUrl = "https://data.binance.vision/data/spot/monthly/klines/";
        private const int startYear = 2017;
        private const int startMonth = 1;
        private readonly int currentYear = DateTime.Today.Year;
        private readonly int currentMonth = DateTime.Today.Month;

        /// <summary>
        /// Put together strings to download monthly data
        /// As soon as we have our file, read the data
        /// Check the amount of entries against the database, if we have the same amount, skip this month
        /// If not then try inserting the data with overwriting existing data just in case they dont match.
        /// This allows the data to be 100% correct compared to Binance
        /// </summary>
        public void StartScrapping(string _connectionString)
        {
            foreach (var asset in (Enums.Assets[])Enum.GetValues(typeof(Enums.Assets)))
            {
                foreach (var timeFrame in (Enums.TimeFrames[])Enum.GetValues(typeof(Enums.TimeFrames)))
                {
                    string url = $"{baseUrl}{asset.GetStringValue().ToUpper()}/{timeFrame.GetStringValue()}/{asset.GetStringValue().ToUpper()}-{timeFrame.GetStringValue()}-";

                    for (int iteratingYear = startYear; iteratingYear <= currentYear; iteratingYear++)
                    {
                        string monthUrl = $"{url}{iteratingYear}-";

                        for (int iteratingMonth = startMonth; iteratingMonth <= 12; iteratingMonth++)
                        {
                            string dayUrl = monthUrl;
                            if (iteratingMonth / 10 < 1)
                            {
                                dayUrl += 0;
                                dayUrl += iteratingMonth;
                            }
                            else
                            {
                                dayUrl += iteratingMonth;
                            }
                            dayUrl += ".zip";

                            Log.Information("{dayUrl}",dayUrl);

                            var data = FileHandler.DownloadFile(dayUrl);
                            if (data.Length != 0)
                            {
                                StreamReader? reader = FileHandler.GetCSVFileStreamReader(data);
                                if (reader != null)
                                {
                                    var assets = new List<Asset>();

                                    string dataToRead;
                                    while ((dataToRead = reader.ReadLine()) != null)
                                    {
                                        var separatedstrings = dataToRead.Split(',');
                                        var dateTimeOpen = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(separatedstrings[0]));
                                        var dateTimeClose = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(separatedstrings[6]));

                                        assets.Add(new Asset
                                        {
                                            AssetName = asset.GetStringValue().ToLower(),
                                            Interval = timeFrame.GetStringValue(),
                                            OpenTime = dateTimeOpen.DateTime,
                                            CandleOpen = Convert.ToDecimal(separatedstrings[1]),
                                            CandleHigh = Convert.ToDecimal(separatedstrings[2]),
                                            CandleLow = Convert.ToDecimal(separatedstrings[3]),
                                            CandleClose = Convert.ToDecimal(separatedstrings[4]),
                                            CloseTime = dateTimeClose.DateTime,
                                            Volume = Convert.ToDecimal(separatedstrings[5]),
                                            QuoteAssetVolume = Convert.ToDecimal(separatedstrings[7]),
                                            Trades = Convert.ToInt64(separatedstrings[8]),
                                            TakerBuyBaseAssetVolume = Convert.ToDecimal(separatedstrings[9]),
                                            TakerBuyQuoteAssetVolume = Convert.ToDecimal(separatedstrings[10])
                                        });
                                    }

                                    Retry.Do(() => DatabaseHandler.UpsertCandles(assets, _connectionString), TimeSpan.FromSeconds(1));
                                }
                            }

                            if (iteratingYear == currentYear && iteratingMonth == currentMonth)
                            {
                                break;
                            }
                        }

                        if (iteratingYear == currentYear)
                        {
                            break;
                        }
                    }
                }
            }
        }
    }
}
