using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using CryptoTradingSystem.General.Data;
using Skender.Stock.Indicators;

namespace CryptoTradingSystem.Scraper
{
    public static class ApiManager
    {
        private static readonly HttpClient Client = new HttpClient();

        public static async Task<List<Quote>> GetBinanceData(Enums.Assets _asset, Enums.TimeFrames _timeFrame, int _limit = 750)
        {
            List<Quote> quotes = new List<Quote>();
            var asset = _asset.GetStringValue().ToUpper();

            var url = $"https://api.binance.com/api/v3/klines?symbol={asset}&interval={_timeFrame.GetStringValue()}&limit={_limit}";

            var response = await Client.GetAsync(url);

            if (!response.IsSuccessStatusCode)
            {
                return quotes;
            }

            var jsonResult = await response.Content.ReadAsStringAsync();
            var datasets = jsonResult.Split("],");

            foreach (var dataset in datasets)
            {
                var entry = dataset.Replace("[", "").Replace("]", "").Replace("\"", "");

                try
                {
                    var separatedStrings = entry.Split(",");

                    quotes.Add(new Quote
                    {
                        Date = DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(separatedStrings[6])).UtcDateTime,
                        Open = Convert.ToDecimal(separatedStrings[1]),
                        High = Convert.ToDecimal(separatedStrings[2]),
                        Low = Convert.ToDecimal(separatedStrings[3]),
                        Close = Convert.ToDecimal(separatedStrings[4]),
                        Volume = Convert.ToDecimal(separatedStrings[5])
                    });
                }
                catch (Exception e)
                {
                    Console.WriteLine($"{DateTime.Now} {e}");
                    throw;
                }
            }

            return quotes;
        }
    }
}
