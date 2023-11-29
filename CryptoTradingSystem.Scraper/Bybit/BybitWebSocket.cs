using bybit.net.api.WebSocketStream;
using CryptoTradingSystem.General.Data;
using CryptoTradingSystem.General.Helper;
using CryptoTradingSystem.Scraper.Interfaces;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Serilog;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CryptoTradingSystem.Scraper.Bybit;

public class BybitWebSocket : IWebSocketManager
{
	private Tuple<DateTime, decimal?> lastCandleClose = new Tuple<DateTime, decimal?>(default, default);

	public async Task CreateWebSocket(Enums.Assets asset, Enums.TimeFrames timeFrame, string connectionString)
	{
		Log.Information(
			"Bybit | {Asset} | {TimeFrame} | open websocket",
			asset.GetStringValue(),
			timeFrame.GetStringValue());

		var interval = timeFrame switch
		{
			Enums.TimeFrames.M5  => "5",
			Enums.TimeFrames.M15 => "15",
			Enums.TimeFrames.H1  => "60",
			Enums.TimeFrames.H4  => "240",
			Enums.TimeFrames.D1  => "D",
			_                    => throw new ArgumentOutOfRangeException(nameof(timeFrame), timeFrame, null)
		};

		var linearWebsocket = new BybitLinearWebSocket(useTestNet: false);
		linearWebsocket.OnMessageReceived(data =>
			{
				HandleMessage(asset, timeFrame, connectionString, data);
				return Task.CompletedTask;
			},
			CancellationToken.None);
		try
		{
			await linearWebsocket.ConnectAsync(new string[] { $"kline.{interval}.{asset.GetStringValue()!.ToUpper()}" }, CancellationToken.None);
		}
		catch (Exception e)
		{
			Log.Error(
				e,
				"Bybit | {Asset} | {TimeFrame} | could not connect to the websocket",
				asset.GetStringValue(),
				timeFrame.GetStringValue());
			throw;
		}
	}

	public void HandleMessage(Enums.Assets asset, Enums.TimeFrames timeFrame, string connectionString, object data)
	{
		JObject json;
		var message = data as string;
		if (string.IsNullOrWhiteSpace(message))
		{
			return;
		}

		try
		{
			json = JObject.Parse(message);
		}
		catch (JsonReaderException e)
		{
			Log.Error(
				e,
				"{Asset} | {TimeFrame} | could not parse callback {Callback}",
				asset.GetStringValue(),
				timeFrame.GetStringValue(),
				message);
			throw;
		}

		if (json["data"] == null)
		{
			return;
		}
		
		var dataObject = json["data"]!.FirstOrDefault();
		if (dataObject == null)
		{
			return;
		}

		var openTime = Convert.ToInt64(dataObject["start"]!.Value<string>());
		var closeTime = Convert.ToInt64(dataObject["end"]!.Value<string>());
		
		var openPriceString = dataObject["open"]!.Value<string>()!;
		var highPriceString = dataObject["high"]!.Value<string>()!;
		var lowPriceString = dataObject["low"]!.Value<string>()!;
		var closePriceString = dataObject["close"]!.Value<string>()!;
		var volumeString = dataObject["volume"]!.Value<string>()!;
		var quoteAssetVolumeString = dataObject["turnover"]!.Value<string>()!;
		//var tradesString = dataObject["n"]!.Value<string>()!;
		//var takerBuyBaseAssetVolumeString = dataObject["V"]!.Value<string>()!;
		//var takerBuyQuoteAssetVolumeString = dataObject["Q"]!.Value<string>()!;
		var dateTimeOpen = DateTimeOffset.FromUnixTimeMilliseconds(openTime);
		var dateTimeClose = DateTimeOffset.FromUnixTimeMilliseconds(closeTime);

		Retry.Do(
			() => DatabaseHandler.UpsertCandles(
				new()
				{
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
						QuoteAssetVolume = Convert.ToDecimal(quoteAssetVolumeString),
						//Trades = Convert.ToInt64(tradesString),
						//TakerBuyBaseAssetVolume = Convert.ToDecimal(takerBuyBaseAssetVolumeString),
						//TakerBuyQuoteAssetVolume = Convert.ToDecimal(takerBuyQuoteAssetVolumeString)
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
						Exchange = Enums.Exchange.Bybit.GetStringValue()!,
						AssetName = asset.GetStringValue(),
						Interval = timeFrame.GetStringValue(),
						OpenTime = dateTimeOpen.DateTime,
						CloseTime = dateTimeClose.DateTime,
						ReturnToLastCandle = lastCandleCloseValue.HasValue
							? Convert.ToDecimal(closePriceString) - lastCandleCloseValue.Value
							: null,
						ReturnToLastCandleInPercentage = lastCandleCloseValue.HasValue
							? (Convert.ToDecimal(closePriceString) - lastCandleCloseValue.Value)
							  / lastCandleCloseValue.Value
							: null
					}
				},
				connectionString),
			TimeSpan.FromSeconds(1));

		if (lastCandleClose.Item1 != dateTimeClose.DateTime)
		{
			lastCandleClose = new(dateTimeClose.DateTime, Convert.ToDecimal(closePriceString));
		}
	}
}