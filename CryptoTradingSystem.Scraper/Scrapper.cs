using CryptoTradingSystem.General.Data;
using CryptoTradingSystem.General.Database.Models;
using CryptoTradingSystem.General.Helper;
using Serilog;
using System;
using System.Collections.Generic;
using System.IO;

namespace CryptoTradingSystem.Scraper
{
    internal class Scrapper
    {
        private const string BaseUrl = "https://data.binance.vision/data/spot/monthly/klines/";
        private const int StartYear = 2017;
        private const int StartMonth = 1;
        private readonly int _currentYear = DateTime.Today.Year;
        private readonly int _currentMonth = DateTime.Today.Month;

        /// <summary>
        /// Put together strings to download monthly data
        /// As soon as we have our file, read the data
        /// Check the amount of entries against the database, if we have the same amount, skip this month
        /// If not then try inserting the data with overwriting existing data just in case they dont match.
        /// This allows the data to be 100% correct compared to Binance
        /// </summary>
        public void StartScrapping(string connectionString)
        {
            foreach (var asset in (Enums.Assets[])Enum.GetValues(typeof(Enums.Assets)))
            {
                decimal? lastCandleClose = null;
                foreach (var timeFrame in (Enums.TimeFrames[])Enum.GetValues(typeof(Enums.TimeFrames)))
                {
                    var url = $"{BaseUrl}{asset.GetStringValue()?.ToUpper()}/{timeFrame.GetStringValue()}/{asset.GetStringValue()?.ToUpper()}-{timeFrame.GetStringValue()}-";

                    for (var iteratingYear = StartYear; iteratingYear <= _currentYear; iteratingYear++)
                    {
                        var monthUrl = $"{url}{iteratingYear}-";

                        for (var iteratingMonth = StartMonth; iteratingMonth <= 12; iteratingMonth++)
                        {
                            var dayUrl = monthUrl;
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

                            Log.Information("{dayUrl}", dayUrl);

                            var data = FileHandler.DownloadFile(dayUrl);
                            if (data.Length != 0)
                            {
                                StreamReader? reader = FileHandler.GetCSVFileStreamReader(data);
                                if (reader != null)
                                {
                                    var assets = new List<Asset>();
                                    var additionalInformations = new List<AssetAdditionalInformation>();

                                    string? dataToRead;
                                    while ((dataToRead = reader.ReadLine()) != null)
                                    {
                                        var separatedstrings = dataToRead.Split(',');
                                        var dateTimeOpen = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(separatedstrings[0]));
                                        var dateTimeClose = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(separatedstrings[6]));

                                        assets.Add(new Asset
                                        {
                                            AssetName = asset.GetStringValue()?.ToLower(),
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

                                        CalculateAdditionalInformations(lastCandleClose,
                                                                        Convert.ToDecimal(separatedstrings[4]),
                                                                        asset.GetStringValue()?.ToLower()!,
                                                                        timeFrame.GetStringValue()!,
                                                                        dateTimeOpen.DateTime,
                                                                        dateTimeClose.DateTime,
                                                                        additionalInformations);
                                    }

                                    Retry.Do(() => DatabaseHandler.UpsertCandles(assets, connectionString), TimeSpan.FromSeconds(1));
                                    Retry.Do(() => DatabaseHandler.UpsertAssetAdditionalInformation(additionalInformations, connectionString), TimeSpan.FromSeconds(1));
                                }
                            }

                            if (iteratingYear == _currentYear && iteratingMonth == _currentMonth)
                            {
                                break;
                            }
                        }

                        if (iteratingYear == _currentYear)
                        {
                            break;
                        }
                    }
                }
            }
        }

        private void CalculateAdditionalInformations(
            decimal? lastCandleClose,
            decimal currentCandleClose,
            string assetName,
            string interval,
            DateTime openTime,
            DateTime closeTime,
            List<AssetAdditionalInformation> additionalInformations)
        {
            var assetAdditionalInformation = new AssetAdditionalInformation
            {
                AssetName = assetName,
                Interval = interval,
                OpenTime = openTime,
                CloseTime = closeTime
            };

            if (lastCandleClose != null)
            {
                assetAdditionalInformation.ReturnToLastCandle = currentCandleClose - lastCandleClose.Value;
                assetAdditionalInformation.ReturnToLastCandleInPercentage = currentCandleClose - lastCandleClose.Value / lastCandleClose.Value;
            }

            additionalInformations.Add(assetAdditionalInformation);
        }
    }
}
