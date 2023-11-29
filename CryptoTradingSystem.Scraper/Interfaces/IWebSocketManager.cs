using CryptoTradingSystem.General.Data;
using System.Threading.Tasks;

namespace CryptoTradingSystem.Scraper.Interfaces;

public interface IWebSocketManager
{
	Task CreateWebSocket(Enums.Assets asset, Enums.TimeFrames timeFrame, string connectionString);
	
	void HandleMessage(Enums.Assets asset, Enums.TimeFrames timeFrame, string connectionString, object data);
}