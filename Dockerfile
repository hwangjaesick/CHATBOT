FROM mcr.microsoft.com/azure-functions/python:4-python3.9

COPY . /home/site/wwwroot
ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true \ 
    AzureWebJobsStorage=DefaultEndpointsProtocol=https;AccountName=lgestordevdatast;AccountKey=BQu2kvDpMxil3CghhaVYs5j8wGMfvmsFQVHiq59pgOcX25hwP64/3tiPtyYQR+O3nYUAqjbZyTJV+ASt3Gy2ng==;EndpointSuffix=core.windows.net \ 
    CORS__AllowedOrigins="[\"*\"]" \
    CONTAINER_NAME="" 

EXPOSE 9000
EXPOSE 7071
EXPOSE 80

RUN cd /home/site/wwwroot && pip install -r ./requirements.txt

