# Quick Start App

The quick start application is a **self-contained and runnable** example script that demonstrates authenticating, connecting to, ingesting data, querying and administering Azure Data Explorer using the azure-kusto-python SDK.

You can use it as a baseline to write your own first kusto client application, altering the code as you go, or copy code sections out of it into your app.

The app includes comments with tips, recommandations on best prectices, reference metrials, and recomanded TODO changes when adapting it to a from a demo to a working app.


## Using the Script 

### Using the App from GitHub
1. Download the app files from GitHub
2. Run: `pip install azure-kusto-data azure-kusto-ingest`
3. Open `kusto_sample_config.json` and modify `KustoUri`, `IngestUri` and `DatabaseName` to reflect you ADX cluster
4. Run the script

### Using the Script from OneClick 
1. Open a browser and type your cluster's address
2. Open the left side menu, if it is closed, using the Hamburger button
3. On the left side menu choose Data
4. Click on ** - - - TBD - - - **
5. Follow the wizard to select your data, target details and language of choice (Python), etc.
6. Download the app as a zip file 
7. Unpack the script to your folder of choice 
8. Run the script (it will already be configured to your cluster and data)

### Optional Changes
1. Withing the script itslef, you may alter the authentication method from prompt by editing `authenticationMode`
2. You can also make the script run without stopping between steps by setting `waitForUser = False`

