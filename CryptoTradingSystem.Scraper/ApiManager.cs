using CryptoTradingSystem.General.Data;
using Serilog;
using Skender.Stock.Indicators;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

namespace CryptoTradingSystem.Scraper;

public static class ApiManager
{
	private static readonly HttpClient Client = new();

	public static async Task<List<Quote>> GetBinanceData(
		Enums.Assets asset,
		Enums.TimeFrames timeFrame,
		int limit = 750)
	{
		var quotes = new List<Quote>();
		var assetName = asset.GetStringValue()?.ToUpper();

		var url =
			$"https://api.binance.com/api/v3/klines?symbol={assetName}&interval={timeFrame.GetStringValue()}&limit={limit}";

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

				quotes.Add(
					new()
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
				Log.Error(e, "could not add to the quotes {Entry}", entry);
				throw;
			}
		}

		return quotes;
	}
}