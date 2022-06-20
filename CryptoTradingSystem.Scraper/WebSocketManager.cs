#nullable enable
using System;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using CryptoTradingSystem.General.Data;
using CryptoTradingSystem.General.Database.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CryptoTradingSystem.Scraper
{
    internal class WebSocketManager
    {
        public void CreateWebSocket(Enums.Assets _asset, Enums.TimeFrames _timeFrame, string _connectionString)
        {
            using var ws = new ClientWebSocket();

            try
            {
                Uri uri = new Uri($"wss://stream.binance.com:9443/ws/{_asset.GetStringValue()}@kline_{_timeFrame.GetStringValue()}");
                ws.ConnectAsync(uri, CancellationToken.None).GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }

            byte[] buffer = new byte[2048];
            while (ws.State == WebSocketState.Open)
            {
                WebSocketReceiveResult? result;
                try
                {
                    result = ws.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None).GetAwaiter().GetResult();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    return;
                }

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    Console.WriteLine($"{_asset.GetStringValue()} | {_timeFrame.GetStringValue()} | close als Message bekommen");

                    ws.CloseAsync(WebSocketCloseStatus.NormalClosure, string.Empty, CancellationToken.None).GetAwaiter().GetResult();
                }
                else
                {
                    HandleMessage(buffer, result.Count, _asset, _timeFrame, _connectionString);
                }
            }
        }

        /// <summary>
        /// Actually insert the candle into the DB
        /// </summary>
        /// <param name="_buffer"></param>
        /// <param name="_count"></param>
        /// <param name="_timeFrame"></param>
        /// <param name="_asset"></param>
        /// <param name="_connectionString"></param>
        private static void HandleMessage(byte[] _buffer, int _count, Enums.Assets _asset, Enums.TimeFrames _timeFrame, string _connectionString)
        {
            string callback = Encoding.UTF8.GetString(_buffer, 0, _count);
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
                catch (Exception exception)
                {
                    Console.WriteLine(exception);
                    throw;
                }
            }

            long openTime = Convert.ToInt64(json["k"]?["t"]!.Value<string>());
            long closeTime = Convert.ToInt64(json["k"]?["T"]!.Value<string>());

            string openPriceString = json["k"]?["o"]!.Value<string>()!;
            string highPriceString = json["k"]?["h"]!.Value<string>()!;
            string lowPriceString = json["k"]?["l"]!.Value<string>()!;
            string closePriceString = json["k"]?["c"]!.Value<string>()!;
            string volumeString = json["k"]?["v"]!.Value<string>()!;
            string quoteAssetVolumeString = json["k"]?["q"]!.Value<string>()!;
            string tradesString = json["k"]?["n"]!.Value<string>()!;
            string TakerBuyBaseAssetVolumeString = json["k"]?["V"]!.Value<string>()!;
            string TakerBuyQuoteAssetVolumeString = json["k"]?["Q"]!.Value<string>()!;
            DateTimeOffset dateTimeOpen = DateTimeOffset.FromUnixTimeMilliseconds(openTime);
            DateTimeOffset dateTimeClose = DateTimeOffset.FromUnixTimeMilliseconds(closeTime);

            DatabaseHandler.UpsertCandles(new List<Asset>
            {
                new Asset
                {
                    AssetName = _asset.GetStringValue(),
                    Interval = _timeFrame.GetStringValue(),
                    OpenTime = dateTimeOpen.DateTime,
                    CandleOpen = Convert.ToDecimal(openPriceString),
                    CandleHigh = Convert.ToDecimal(highPriceString),
                    CandleLow = Convert.ToDecimal(lowPriceString),
                    CandleClose = Convert.ToDecimal(closePriceString),
                    CloseTime = dateTimeClose.DateTime,
                    Volume = Convert.ToDecimal(volumeString),
                    QuoteAssetVolume = Convert.ToDecimal(quoteAssetVolumeString),
                    Trades = Convert.ToInt64(tradesString),
                    TakerBuyBaseAssetVolume = Convert.ToDecimal(TakerBuyBaseAssetVolumeString),
                    TakerBuyQuoteAssetVolume = Convert.ToDecimal(TakerBuyQuoteAssetVolumeString)
                }
            }, _connectionString);
        }
    }
}
