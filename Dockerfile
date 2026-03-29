FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /source
COPY . .
ARG PROJECT=LedgerFlow.Api
RUN dotnet publish src/${PROJECT}/${PROJECT}.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app/publish .
ARG PROJECT=LedgerFlow.Api
ENV LEDGERFLOW_PROJECT=${PROJECT}
ENTRYPOINT ["sh", "-c", "dotnet /app/${LEDGERFLOW_PROJECT}.dll"]
