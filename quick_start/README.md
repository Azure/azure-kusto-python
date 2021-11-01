# Quick Start App

The quick start application is a **self-contained and runnable** example script that demonstrates authenticating, connecting to, administering, ingesting data into and querying Azure Data Explorer using the azure-kusto-python SDK.
You can use it as a baseline to write your own first kusto client application, altering the code as you go, or copy code sections out of it into your app.

**Tip:** The app includes comments with tips on recommendations, coding best practices, links to reference materials and recommended TODO changes when adapting the code to your needs.


## Using the App for the first time

### Using the App from GitHub
1. Download the app files from GitHub
2. Run: `python -m pip install azure-kusto-data azure-kusto-ingest`
3. Open `kusto_sample_config.json` and modify `KustoUri`, `IngestUri` and `DatabaseName` to reflect you ADX cluster
4. Run the script

### Using the App from OneClick 
1. Open a browser and type your cluster's address, you will be redirected to the _Azure Data Explorer_ website 
2. Open the left side menu, if it is closed, using the Hamburger button
3. On the left side menu choose _Data_
4. Click on _Create Sample App Code_ button 
5. Follow the wizard
6. Download the app as a zip file 
7. Unpack the script to your folder of choice
8. Open a command line windows and CD into the folder you created 
9. Run `python -m pip install azure-kusto-data azure-kusto-ingest`
10. Run `python kusto_sample_app.py`. It will already be configured to your cluster and source.

### Optional Changes
1. Withing the script itself, you may alter the default User-Prompt authentication method by editing `authenticationMode`
2. You can also make the script run without stopping between steps by setting `waitForUser = False`

###Troubleshooting
* If you are having trouble running python on your machine or need instructions on how to install python, you can consult the following [python environment setup tutorial](https://www.tutorialspoint.com/python/python_environment.htm) from _TutorialsPoint_.
* If you are having trouble running the script from your IDE of choice, first check if the script runs from commandline, then consult the troubleshooting references of your IDE.   

