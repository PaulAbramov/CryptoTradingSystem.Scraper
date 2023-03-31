FROM mcr.microsoft.com/dotnet/sdk:7.0
WORKDIR /app

COPY . .

ARG TARGETPLATFORM=linux/amd64

RUN dotnet restore
RUN dir bin/Debug/net7.0 -s
COPY ["bin/Debug/net7.0/CryptoTradingSystem.General.dll", "."]

RUN dotnet publish -c Release -o /out -r $TARGETPLATFORM

ENTRYPOINT ["dotnet", "run", "-c", "Release"]