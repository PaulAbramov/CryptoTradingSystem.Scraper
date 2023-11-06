#nullable enable
using CryptoTradingSystem.General.Data;
using CryptoTradingSystem.General.Helper;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Serilog;
using System;
using System.Net.WebSockets;
using System.Text;
using System.Threading;

namespace CryptoTradingSystem.Scraper;

public class WebSocketManager
{
	public void CreateWebSocket(Enums.Assets asset, Enums.TimeFrames timeFrame, string connectionString)
	{
		var lastCandleClose = new Tuple<DateTime, decimal?>(default, default);

		Log.Information("{Asset} | {TimeFrame} | open websocket", asset.GetStringValue(), timeFrame.GetStringValue());

		using var ws = new ClientWebSocket();

		try
		{
			var uri = new Uri(
				$"wss://stream.binance.com:9443/ws/{asset.GetStringValue()}@kline_{timeFrame.GetStringValue()}");
			ws.ConnectAsync(uri, CancellationToken.None).GetAwaiter().GetResult();
		}
		catch (Exception e)
		{
			Log.Error(
				e,
				"{Asset} | {TimeFrame} | could not connect to the websocket",
				asset.GetStringValue(),
				timeFrame.GetStringValue());
			throw;
		}

		var buffer = new byte[2048];
		while (ws.State == WebSocketState.Open)
		{
			WebSocketReceiveResult? result;
			try
			{
				result = ws.ReceiveAsync(new(buffer), CancellationToken.None).GetAwaiter().GetResult();
			}
			catch (Exception e)
			{
				if (e.Message.Contains(
					    "The remote party closed the WebSocket connection without completing the close handshake"))
				{
					Log.Information(
						e,
						"{Asset} | {TimeFrame} | closed without completing the handshake",
						asset.GetStringValue(),
						timeFrame.GetStringValue());
					ws.CloseAsync(WebSocketCloseStatus.NormalClosure, string.Empty, CancellationToken.None)
						.GetAwaiter()
						.GetResult();
				}
				else
				{
					Log.Error(
						e,
						"{Asset} | {TimeFrame} | could not receive message from remote",
						asset.GetStringValue(),
						timeFrame.GetStringValue());
				}

				return;
			}

			if (result.MessageType == WebSocketMessageType.Close)
			{
				Log.Warning(
					"{Asset} | {TimeFrame} | received close message",
					asset.GetStringValue(),
					timeFrame.GetStringValue());

				ws.CloseAsync(WebSocketCloseStatus.NormalClosure, string.Empty, CancellationToken.None)
					.GetAwaiter()
					.GetResult();
			}
			else
			{
				HandleMessage(buffer, result.Count, asset, timeFrame, connectionString, ref lastCandleClose);
			}
		}
	}

    /// <summary>
    ///   Actually insert the candle into the DB
    /// </summary>
    /// <param name="buffer"></param>
    /// <param name="count"></param>
    /// <param name="timeFrame"></param>
    /// <param name="asset"></param>
    /// <param name="connectionString"></param>
    private void HandleMessage(
		byte[] buffer,
		int count,
		Enums.Assets asset,
		Enums.TimeFrames timeFrame,
		string connectionString,
		ref Tuple<DateTime, decimal?> lastCandleClose)
	{
		var callback = Encoding.UTF8.GetString(buffer, 0, count);
		JObject json;

		try
		{
			json = JObject.Parse(callback);
		}
		catch (JsonReaderException e)
		{
			try
			{
				var callbackLength = callback.Length;
				var fixedCallback = callback.Remove(callbackLength - 2, 1);

				json = JObject.Parse(fixedCallback);
			}
			catch (Exception ex)
			{
				Log.Error(
					ex,
					"{Asset} | {TimeFrame} | could not parse callback {Callback}",
					asset.GetStringValue(),
					timeFrame.GetStringValue(),
					callback);
				throw;
			}
		}

		var openTime = Convert.ToInt64(json["k"]?["t"]!.Value<string>());
		var closeTime = Convert.ToInt64(json["k"]?["T"]!.Value<string>());

		var openPriceString = json["k"]?["o"]!.Value<string>()!;
		var highPriceString = json["k"]?["h"]!.Value<string>()!;
		var lowPriceString = json["k"]?["l"]!.Value<string>()!;
		var closePriceString = json["k"]?["c"]!.Value<string>()!;
		var volumeString = json["k"]?["v"]!.Value<string>()!;
		var quoteAssetVolumeString = json["k"]?["q"]!.Value<string>()!;
		var tradesString = json["k"]?["n"]!.Value<string>()!;
		var takerBuyBaseAssetVolumeString = json["k"]?["V"]!.Value<string>()!;
		var takerBuyQuoteAssetVolumeString = json["k"]?["Q"]!.Value<string>()!;
		var dateTimeOpen = DateTimeOffset.FromUnixTimeMilliseconds(openTime);
		var dateTimeClose = DateTimeOffset.FromUnixTimeMilliseconds(closeTime);

		Retry.Do(
			() => DatabaseHandler.UpsertCandles(
				new()
				{
					new()
					{
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
						Trades = Convert.ToInt64(tradesString),
						TakerBuyBaseAssetVolume = Convert.ToDecimal(takerBuyBaseAssetVolumeString),
						TakerBuyQuoteAssetVolume = Convert.ToDecimal(takerBuyQuoteAssetVolumeString)
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