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
                using var contextDB = new CryptoTradingSystemContext(connectionString);
                contextDB.Database.EnsureCreated();
            }
            catch (ArgumentException e)
            {
                Log.Error(e, "Connectionstring is not correct: '{connectionString}'", connectionString);
                throw;
            }
            catch (Exception e)
            {
                Log.Error(e, "could not ensure the creation of the database '{connectionString}'", connectionString);
                throw;
            }
        }

        public static void UpsertCandles(List<Asset> assets, string connectionString)
        {
            try
            {
                using var contextDB = new CryptoTradingSystemContext(connectionString);
                using var transaction = contextDB.Database.BeginTransaction();

                foreach (var asset in assets)
                {
                    var candle = contextDB.Assets?.FirstOrDefault(x => 
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
                        contextDB.Assets?.Add(asset);
                    }
                }
                contextDB.SaveChanges();
                transaction.Commit();
            }
            catch (Exception e)
            {
                var asset = assets.FirstOrDefault();
                Log.Error("{asset} | {timeFrame} | Could not do the upsert transaction.", asset.AssetName, asset.Interval);
                throw;
            }
        }
    }
}
