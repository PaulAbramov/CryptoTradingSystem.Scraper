using Binance.Net.Clients;
using Binance.Net.Enums;
using CryptoTradingSystem.General.Data;
using CryptoTradingSystem.General.Database.Models;
using CryptoTradingSystem.General.Helper;
using CryptoTradingSystem.Scraper.Interfaces;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CryptoTradingSystem.Scraper.Binance;

public class BinanceScraper : IScraper
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
			Enums.TimeFrames.M5  => KlineInterval.FiveMinutes,
			Enums.TimeFrames.M15 => KlineInterval.FifteenMinutes,
			Enums.TimeFrames.H1  => KlineInterval.OneHour,
			Enums.TimeFrames.H4  => KlineInterval.FourHour,
			Enums.TimeFrames.D1  => KlineInterval.OneDay,
			var _                => throw new ArgumentOutOfRangeException(nameof(timeFrame), timeFrame, null)
		};

		var client = new BinanceRestClient();
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
		BinanceRestClient client,
		Enums.Assets asset,
		Enums.TimeFrames timeFrame,
		string connectionString,
		decimal? lastCandleClose,
		DateTime startTime,
		KlineInterval interval,
		List<Asset> assets,
		List<AssetAdditionalInformation> additionalInformations)
	{
		var result = await client
			.UsdFuturesApi
			.ExchangeData
			.GetContinuousContractKlinesAsync(
				asset.GetStringValue()!,
				ContractType.Perpetual,
				interval,
				startTime);

		var lastCandle = result.Data.LastOrDefault();
		if (lastCandle == null)
		{
			return startTime;
		}

		startTime = lastCandle.CloseTime;
		Log.Information("Binance | {asset} | {timeframe} last candle is: {lastCandle}",
			asset.GetStringValue(),
			timeFrame.GetStringValue(),
			lastCandle.CloseTime);

		var klineData = result.Data;
		foreach (var data in klineData)
		{
			assets.Add(
				new()
				{
					Exchange = Enums.Exchange.Binance.GetStringValue()!,
					AssetName = asset.GetStringValue()?.ToLower(),
					Interval = timeFrame.GetStringValue(),
					OpenTime = data.OpenTime,
					CandleOpen = data.OpenPrice,
					CandleHigh = data.HighPrice,
					CandleLow = data.LowPrice,
					CandleClose = data.ClosePrice,
					CloseTime = data.CloseTime,
					Volume = data.Volume,
					QuoteAssetVolume = data.QuoteVolume,
					Trades = data.TradeCount,
					TakerBuyBaseAssetVolume = data.TakerBuyBaseVolume,
					TakerBuyQuoteAssetVolume = data.TakerBuyQuoteVolume
				});

			CalculateAdditionalInformations(
				lastCandleClose,
				data.ClosePrice,
				asset.GetStringValue()?.ToLower()!,
				timeFrame.GetStringValue()!,
				data.OpenTime,
				data.CloseTime,
				additionalInformations);
		}

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
			Exchange = Enums.Exchange.Binance.GetStringValue()!,
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