using bybit.net.api;
using bybit.net.api.ApiServiceImp;
using bybit.net.api.Models;
using bybit.net.api.Models.Market;
using CryptoTradingSystem.General.Data;
using CryptoTradingSystem.General.Database.Models;
using CryptoTradingSystem.General.Helper;
using CryptoTradingSystem.Scraper.Interfaces;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CryptoTradingSystem.Scraper.Bybit;

public class BybitScraper : IScraper
{
	private const int startYear = 2017;
	private const int startMonth = 5;

	public async Task StartScrapping(string connectionString)
	{
		foreach (var asset in (Enums.Assets[]) Enum.GetValues(typeof(Enums.Assets)))
		{
			decimal? lastCandleClose = null;
			foreach (var timeFrame in (Enums.TimeFrames[]) Enum.GetValues(typeof(Enums.TimeFrames)))
			{
				await GetDataThroughAPI(asset, timeFrame, connectionString, lastCandleClose);
			}
		}
	}
	
	private async Task GetDataThroughAPI(
		Enums.Assets asset,
		Enums.TimeFrames timeFrame,
		string connectionString,
		decimal? lastCandleClose = null)
	{
		var interval = timeFrame switch
		{
			Enums.TimeFrames.M5  => MarketInterval.FiveMinutes,
			Enums.TimeFrames.M15 => MarketInterval.FifteenMinutes,
			Enums.TimeFrames.H1  => MarketInterval.OneHour,
			Enums.TimeFrames.H4  => MarketInterval.FourHours,
			Enums.TimeFrames.D1  => MarketInterval.Daily,
			var _                => throw new ArgumentOutOfRangeException(nameof(timeFrame), timeFrame, null)
		};

		var client = new BybitMarketDataService(url: BybitConstants.HTTP_MAINNET_URL);
		var startTime = new DateTime(startYear, startMonth, 1);
		var assets = new List<Asset>();
		var additionalInformations = new List<AssetAdditionalInformation>();
		while (startTime.Date != DateTime.Today)
		{
			startTime = await ParseAndUploadData(
				client,
				asset,
				timeFrame,
				connectionString,
				lastCandleClose,
				startTime,
				interval,
				assets,
				additionalInformations);
		}
	}
	
	private async Task<DateTime> ParseAndUploadData(
		BybitMarketDataService client,
		Enums.Assets asset,
		Enums.TimeFrames timeFrame,
		string connectionString,
		decimal? lastCandleClose,
		DateTime startTime,
		MarketInterval interval,
		List<Asset> assets,
		List<AssetAdditionalInformation> additionalInformations)
	{
		var result = await client.GetMarketKline(
			Category.LINEAR,
			asset.GetStringValue()!.ToUpper(),
			interval,
			((DateTimeOffset)startTime).ToUnixTimeMilliseconds());
		
		JObject json;
		if (string.IsNullOrWhiteSpace(result))
		{
			return startTime;
		}

		try
		{
			json = JObject.Parse(result);
		}
		catch (JsonReaderException e)
		{
			Log.Error(
				e,
				"Bybit | {Asset} | {TimeFrame} | could not parse callback {Callback}",
				asset.GetStringValue(),
				timeFrame.GetStringValue(),
				result);
			throw;
		}

		if (json["result"]?["list"] == null)
		{
			return startTime;
		}

		for (var i = json["result"]["list"].Count() - 1; i >= 0; i--)
		{
			var data = json["result"]["list"][i];
			
			var openTime = Convert.ToInt64(data[0]!.Value<string>());

			var openPriceString = data[1]!.Value<string>()!;
			var highPriceString = data[2]!.Value<string>()!;
			var lowPriceString = data[3]!.Value<string>()!;
			var closePriceString = data[4]!.Value<string>()!;
			var volumeString = data[5]!.Value<string>()!;
			var quoteAssetVolumeString = data[6]!.Value<string>()!;

			//var tradesString = dataObject["n"]!.Value<string>()!;
			//var takerBuyBaseAssetVolumeString = dataObject["V"]!.Value<string>()!;
			//var takerBuyQuoteAssetVolumeString = dataObject["Q"]!.Value<string>()!;
			var dateTimeOpen = DateTimeOffset.FromUnixTimeMilliseconds(openTime);
			
			var minutesToAdd = interval.Value switch
			{
				"D" => 1 * 60 * 24,
				"W" => 1* 60 * 24 * 7,
				_   => Convert.ToDouble(interval.Value)
			};

			var dateTimeClose = dateTimeOpen + TimeSpan.FromMinutes(minutesToAdd);
			
			assets.Add(
				new()
				{
					Exchange = Enums.Exchange.Bybit.GetStringValue()!,
					AssetName = asset.GetStringValue(),
					Interval = timeFrame.GetStringValue(),
					OpenTime = dateTimeOpen.DateTime,
					CandleOpen = Convert.ToDecimal(openPriceString),
					CandleHigh = Convert.ToDecimal(highPriceString),
					CandleLow = Convert.ToDecimal(lowPriceString),
					CandleClose = Convert.ToDecimal(closePriceString),
					CloseTime = dateTimeClose.DateTime,
					Volume = Convert.ToDecimal(volumeString),
					QuoteAssetVolume = Convert.ToDecimal(quoteAssetVolumeString)

					//Trades = Convert.ToInt64(tradesString),
					//TakerBuyBaseAssetVolume = Convert.ToDecimal(takerBuyBaseAssetVolumeString),
					//TakerBuyQuoteAssetVolume = Convert.ToDecimal(takerBuyQuoteAssetVolumeString)
				});

			CalculateAdditionalInformations(
				lastCandleClose,
				Convert.ToDecimal(closePriceString),
				asset.GetStringValue()?.ToLower()!,
				timeFrame.GetStringValue()!,
				dateTimeOpen.DateTime,
				dateTimeClose.DateTime,
				additionalInformations);
			
			startTime = dateTimeClose.DateTime;
		}
		
		Log.Information("Bybit | {asset} | {timeframe} last candle is: {lastCandle}",
			asset.GetStringValue(),
			timeFrame.GetStringValue(),
			startTime);
		
		Retry.Do(
			() => DatabaseHandler.UpsertCandles(assets, connectionString),
			TimeSpan.FromSeconds(1));
		Retry.Do(
			() => DatabaseHandler.UpsertAssetAdditionalInformation(
				additionalInformations,
				connectionString),
			TimeSpan.FromSeconds(1));

		return startTime;
	}
	
	private void CalculateAdditionalInformations(
		decimal? lastCandleClose,
		decimal currentCandleClose,
		string assetName,
		string interval,
		DateTime openTime,
		DateTime closeTime,
		ICollection<AssetAdditionalInformation> additionalInformations)
	{
		var assetAdditionalInformation = new AssetAdditionalInformation
		{
			Exchange = Enums.Exchange.Bybit.GetStringValue()!,
			AssetName = assetName,
			Interval = interval,
			OpenTime = openTime,
			CloseTime = closeTime
		};

		if (lastCandleClose != null)
		{
			assetAdditionalInformation.ReturnToLastCandle = currentCandleClose - lastCandleClose.Value;
			assetAdditionalInformation.ReturnToLastCandleInPercentage =
				currentCandleClose - lastCandleClose.Value / lastCandleClose.Value;
		}

		additionalInformations.Add(assetAdditionalInformation);
	}
}