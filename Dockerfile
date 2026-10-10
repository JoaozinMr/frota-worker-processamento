FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY src/FrotaWorker/FrotaWorker.csproj src/FrotaWorker/
RUN dotnet restore src/FrotaWorker/FrotaWorker.csproj
COPY src/FrotaWorker/ src/FrotaWorker/
RUN dotnet publish src/FrotaWorker/FrotaWorker.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
  CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/8080 && printf "GET /health HTTP/1.0\r\nHost: localhost\r\n\r\n" >&3 && head -n1 <&3 | grep -q 200' || exit 1
USER app
ENTRYPOINT ["dotnet", "FrotaWorker.dll"]
