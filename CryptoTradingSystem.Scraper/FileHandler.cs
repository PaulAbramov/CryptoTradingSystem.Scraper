using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;

namespace CryptoTradingSystem.Scraper
{
    static class FileHandler
    {
        public static byte[] DownloadFile(string _url)
        {
            byte[] data = new byte[]{};
            using (var client = new WebClient())
            {
                try
                {
                    data = client.DownloadData(_url);
                }
                catch (Exception e)
                {
                    if (!e.Message.Contains("404"))
                    {
                        Console.WriteLine(e);
                    }
                }
            }

            return data;
        }

        public static StreamReader? GetCSVFileStreamReader(byte[] _data)
        {
            Stream stream = new MemoryStream(_data);
            ZipArchive archive = new ZipArchive(stream);

            var csvFile = archive.Entries.FirstOrDefault();

            if (csvFile != null)
            {
                var fileData = csvFile.Open();

                return new StreamReader(fileData);
            }

            return null;
        }
    }
}
