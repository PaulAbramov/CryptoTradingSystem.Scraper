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
        public static void InitializeDatabase(string connectionString)
        {
            try
            {
                using var contextDb = new CryptoTradingSystemContext(connectionString);
                contextDb.Database.EnsureCreated();
            }
            catch (ArgumentException e)
            {
                Log.Error(e, "Connectionstring is not correct: '{ConnectionString}'", connectionString);
                throw;
            }
            catch (Exception e)
            {
                Log.Error(e, "could not ensure the creation of the database '{ConnectionString}'", connectionString);
                throw;
            }
        }

        public static void UpsertCandles(List<Asset> assets, string connectionString)
        {
            try
            {
                using var contextDb = new CryptoTradingSystemContext(connectionString);
                using var transaction = contextDb.Database.BeginTransaction();

                foreach (var asset in assets)
                {
                    var candle = contextDb.Assets?.FirstOrDefault(x => 
                        x.AssetName == asset.AssetName && 
                        x.Interval == asset.Interval && 
                        x.OpenTime == asset.OpenTime && 
                        x.CloseTime == asset.CloseTime);

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
                        contextDb.Assets?.Add(asset);
                    }
                }
                contextDb.SaveChanges();
                transaction.Commit();
            }
            catch (Exception e)
            {
                var asset = assets.FirstOrDefault();
                Log.Error(e, "{Asset} | {TimeFrame} | Could not do the upsert transaction",
                    asset?.AssetName, asset?.Interval);
                throw;
            }
        }
    }
}
