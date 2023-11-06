using Serilog;
using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;

namespace CryptoTradingSystem.Scraper;

public static class FileHandler
{
	public static byte[] DownloadFile(string url)
	{
		var data = Array.Empty<byte>();
		using var client = new WebClient();
		try
		{
			data = client.DownloadData(url);
		}
		catch (Exception e)
		{
			if (!e.Message.Contains("404"))
			{
				Log.Error(e, "error while downloading data, and not 404 error (file not found) : {url}", url);
			}
		}

		return data;
	}

	public static StreamReader? GetCSVFileStreamReader(byte[] data)
	{
		Stream stream = new MemoryStream(data);
		var archive = new ZipArchive(stream);

		var csvFile = archive.Entries.FirstOrDefault();

		if (csvFile == null)
		{
			return null;
		}

		var fileData = csvFile.Open();

		return new(fileData);
	}
}