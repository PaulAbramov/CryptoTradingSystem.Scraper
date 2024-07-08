using System.Threading.Tasks;

namespace CryptoTradingSystem.Scraper.Interfaces;

public interface IScraper
{
	Task StartScrapping(string connectionString);
}