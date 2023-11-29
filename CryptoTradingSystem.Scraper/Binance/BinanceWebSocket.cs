using Binance.Net.Clients;
using Binance.Net.Enums;
using Binance.Net.Interfaces;
using CryptoTradingSystem.General.Data;
using CryptoTradingSystem.General.Helper;
using CryptoTradingSystem.Scraper.Interfaces;
using Serilog;
using System;
using System.Threading.Tasks;

namespace CryptoTradingSystem.Scraper.Binance;

public class BinanceWebSocket : IWebSocketManager
{
	private Tuple<DateTime, decimal?> lastCandleClose = new Tuple<DateTime, decimal?>(default, default);

	public async Task CreateWebSocket(Enums.Assets asset, Enums.TimeFrames timeFrame, string connectionString)
	{
		Log.Information(
			"Binance | {Asset} | {TimeFrame} | open websocket",
			asset.GetStringValue(),
			timeFrame.GetStringValue());

		var interval = timeFrame switch
		{
			Enums.TimeFrames.M5  => KlineInterval.FiveMinutes,
			Enums.TimeFrames.M15 => KlineInterval.FifteenMinutes,
			Enums.TimeFrames.H1  => KlineInterval.OneHour,
			Enums.TimeFrames.H4  => KlineInterval.FourHour,
			Enums.TimeFrames.D1  => KlineInterval.OneDay,
			_                    => throw new ArgumentOutOfRangeException(nameof(timeFrame), timeFrame, null)
		};

		try
		{
			var client = new BinanceSocketClient();  
			var result = await client.UsdFuturesApi.SubscribeToKlineUpdatesAsync(asset.GetStringValue()!, interval, onMessage: async data =>
			{
				HandleMessage(asset, timeFrame, connectionString, data.Data);
			});
			
			result.Data.ConnectionLost += async () =>
			{
				await Task.Delay(1000);
				await result.Data.ReconnectAsync();
			};
			result.Data.ConnectionClosed += async () =>
			{
				await Task.Delay(1000);
				await result.Data.ReconnectAsync();
			};
		}
		catch (Exception e)
		{
			Log.Error(
				e,
				"Binance | {Asset} | {TimeFrame} | could not connect to the websocket",
				asset.GetStringValue(),
				timeFrame.GetStringValue());
			throw;
		}
	}

	public void HandleMessage(Enums.Assets asset, Enums.TimeFrames timeFrame, string connectionString, object data)
	{
		var klineData = ((IBinanceStreamKlineData) data).Data;

		Retry.Do(
			() => DatabaseHandler.UpsertCandles(
				new()
				{
					new()
					{
						Exchange = Enums.Exchange.Binance.GetStringValue()!,
						AssetName = asset.GetStringValue(),
						Interval = timeFrame.GetStringValue(),
						OpenTime = klineData.OpenTime,
						CandleOpen = klineData.OpenPrice,
						CandleHigh = klineData.HighPrice,
						CandleLow = klineData.LowPrice,
						CandleClose = klineData.ClosePrice,
						CloseTime = klineData.CloseTime,
						Volume = klineData.Volume,
						QuoteAssetVolume = klineData.QuoteVolume,
						Trades = klineData.TradeCount,
						TakerBuyBaseAssetVolume = klineData.TakerBuyBaseVolume,
						TakerBuyQuoteAssetVolume = klineData.TakerBuyQuoteVolume
					}
				},
				connectionString),
			TimeSpan.FromSeconds(1));

		var lastCandleCloseValue = lastCandleClose.Item2;

		Retry.Do(
			() => DatabaseHandler.UpsertAssetAdditionalInformation(
				new()
				{
					new()
					{
						Exchange = Enums.Exchange.Binance.GetStringValue()!,
						AssetName = asset.GetStringValue(),
						Interval = timeFrame.GetStringValue(),
						OpenTime = 	klineData.OpenTime,
						CloseTime = klineData.CloseTime,
						ReturnToLastCandle = lastCandleCloseValue.HasValue
							? klineData.ClosePrice - lastCandleCloseValue.Value
							: null,
						ReturnToLastCandleInPercentage = lastCandleCloseValue.HasValue
							? (klineData.ClosePrice - lastCandleCloseValue.Value)
							  / lastCandleCloseValue.Value
							: null
					}
				},
				connectionString),
			TimeSpan.FromSeconds(1));

		if (lastCandleClose.Item1 != klineData.CloseTime)
		{
			lastCandleClose = new(klineData.CloseTime, klineData.ClosePrice);
		}
	}
}