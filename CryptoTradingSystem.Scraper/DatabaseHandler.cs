using System;
using System.Collections.Generic;
using System.Linq;
using CryptoTradingSystem.General.Database;
using CryptoTradingSystem.General.Database.Models;
using Serilog;

namespace CryptoTradingSystem.Scraper
{
    internal static class DatabaseHandler
    {
        public static void InitializeDatabase(string _connectionString)
        {
            using CryptoTradingSystemContext contextDB = new CryptoTradingSystemContext(_connectionString);
            contextDB.Database.EnsureCreated();
        }

        public static void UpsertCandles(List<Asset> _assets, string _connectionString)
        {
            try
            {
                using CryptoTradingSystemContext contextDB = new CryptoTradingSystemContext(_connectionString);
                using var transaction = contextDB.Database.BeginTransaction();

                foreach (var asset in _assets)
                {
                    var candle = contextDB.Assets.FirstOrDefault(x => x.AssetName == asset.AssetName && x.Interval == asset.Interval && x.OpenTime == asset.OpenTime && x.CloseTime == asset.CloseTime);

                    if (candle != null)
                    {
                        candle.CandleClose = asset.CandleClose;
                        candle.CandleLow = asset.CandleLow;
                        candle.CandleHigh = asset.CandleHigh;
                        candle.QuoteAssetVolume = asset.QuoteAssetVolume;
                        candle.Volume = asset.Volume;
                        candle.Trades = asset.Trades;
                        candle.TakerBuyBaseAssetVolume = asset.TakerBuyBaseAssetVolume;
                        candle.TakerBuyQuoteAssetVolume = asset.TakerBuyQuoteAssetVolume;
                    }
                    else
                    {
                        contextDB.Assets.Add(asset);
                    }
                }
                contextDB.SaveChanges();
                transaction.Commit();
            }
            catch (Exception e)
            {
                Log.Error(e, "could not do the upsert transaction of {assets}", _assets);
                throw;
            }
        }
    }
}
