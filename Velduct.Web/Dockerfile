FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["Velduct.Web/Velduct.Web.csproj", "Velduct.Web/"]
RUN dotnet restore "Velduct.Web/Velduct.Web.csproj"
COPY . .
WORKDIR "/src/Velduct.Web"
RUN dotnet build "Velduct.Web.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Velduct.Web.csproj" -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app
COPY --from=publish /app/publish .

# Create log directories — NLog writes to /app/logs/...
RUN mkdir -p /app/logs/all/archives \
             /app/logs/errors/archives \
             /app/logs/audit/archives

EXPOSE 8080
ENTRYPOINT ["dotnet", "Velduct.Web.dll"]
