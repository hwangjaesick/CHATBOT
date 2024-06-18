FROM mcr.microsoft.com/azure-functions/python:4-python3.9

COPY . /home/site/wwwroot
ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true \ 
    AzureWebJobsStorage=DefaultEndpointsProtocol=https;AccountName=lgestorprddatast;AccountKey=GndnL++U4+CaTerKFNLHsRQCB4JvI6MH/SOQVE6cayQnG8zE+l6ShUvDEYdAez/C0QT1fAuqLggv+ASt5am5bw==;EndpointSuffix=core.windows.net \ 
    CORS__AllowedOrigins="[\"*\"]" \
    CONTAINER_NAME="" 

EXPOSE 8091

RUN cd /home/site/wwwroot && pip install -r ./requirements.txt

