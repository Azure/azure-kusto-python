# Quickstart App

The quick start application is a **self-contained and runnable** example script that demonstrates authenticating, connecting to, administering, ingesting data into and querying Azure Data Explorer using the azure-kusto-python SDK.
You can use it as a baseline to write your own first kusto client application, altering the code as you go, or copy code sections out of it into your app.

**Tip:** The app includes comments with tips on recommendations, coding best practices, links to reference materials and recommended TODO changes when adapting the code to your needs.


## Using the App for the first time

### Prerequisites

1. Set up Python version 3.7 or higher on your machine. For instructions, consult a Python environment setup tutorial, like [this](https://www.tutorialspoint.com/python/python_environment.htm).

### Retrieving the app from GitHub

1. Download the app files from this GitHub repo.
1. Modify the `kusto_sample_config.json` file, changing `KustoUri`, `IngestUri` and `DatabaseName` appropriately for your cluster.

### Retrieving the app from OneClick

1. Open a browser and type your cluster's URL (e.g. https://mycluster.westeurope.kusto.windows.net/). You will be redirected to the _Azure Data Explorer_ Web UI.
1. On the left menu, select **Data**.
1. Click **Generate Sample App Code** and then follow the instructions in the wizard.
1. Download the app as a ZIP file.
1. Extract the app source code.
**Note**: The configuration parameters defined in the `kusto_sample_config.json` file are preconfigured with the appropriate values for your cluster. Verify that these are correct.

### Run the app

1. Open a command line window and navigate to the folder where you extracted the script.
1. Run `python -m pip install -r requirements.txt`. If upgrading, append ` --upgrade`.
1. Run `python sample_app.py`.

#### Troubleshooting

* If you are having trouble running the script from your IDE, first check if the script runs from the command line, then consult the troubleshooting references of your IDE.

### Optional changes

You can make the following changes from within the script itself:

- Change the location of the configuration file by editing `CONFIG_FILE_NAME`.
