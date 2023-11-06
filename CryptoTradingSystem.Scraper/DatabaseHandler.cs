using CryptoTradingSystem.General.Database;
using CryptoTradingSystem.General.Database.Models;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CryptoTradingSystem.Scraper;

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
				var candle = contextDb.Assets?.FirstOrDefault(
					x =>
						x.AssetName == asset.AssetName
						&& x.Interval == asset.Interval
						&& x.OpenTime == asset.OpenTime
						&& x.CloseTime == asset.CloseTime);

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
			Log.Error(
				e,
				"{Asset} | {TimeFrame} | Could not do the upsert transaction",
				asset?.AssetName,
				asset?.Interval);
			throw;
		}
	}

	public static void UpsertAssetAdditionalInformation(
		List<AssetAdditionalInformation> additionalInformations,
		string connectionString)
	{
		try
		{
			using var contextDb = new CryptoTradingSystemContext(connectionString);
			using var transaction = contextDb.Database.BeginTransaction();

			foreach (var assetadditionalInformation in additionalInformations)
			{
				var additionalInformation = contextDb.AssetAdditionalInformations?.FirstOrDefault(
					x =>
						x.AssetName == assetadditionalInformation.AssetName
						&& x.Interval == assetadditionalInformation.Interval
						&& x.OpenTime == assetadditionalInformation.OpenTime
						&& x.CloseTime == assetadditionalInformation.CloseTime);

				if (additionalInformation != null)
				{
					additionalInformation.ReturnToLastCandle = assetadditionalInformation.ReturnToLastCandle;
					additionalInformation.ReturnToLastCandleInPercentage =
						assetadditionalInformation.ReturnToLastCandleInPercentage;
				}
				else
				{
					contextDb.AssetAdditionalInformations?.Add(assetadditionalInformation);
				}
			}

			contextDb.SaveChanges();
			transaction.Commit();
		}
		catch (Exception e)
		{
			var asset = additionalInformations.FirstOrDefault();
			Log.Error(
				e,
				"{Asset} | {TimeFrame} | Could not do the additionalinformation upsert transaction",
				asset?.AssetName,
				asset?.Interval);
			throw;
		}
	}
}