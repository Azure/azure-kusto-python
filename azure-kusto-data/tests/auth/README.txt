This script faciliates testing various Auth options, especially those that require running on azure.
This README will focus on testing MSI authentication.

The simplest way to run MSI tests is as follows:
1. Get a dev Kusto cluster 
2. USe Kops to login to the DM node
3. Copy 'set_auth_env.bat' and 'run_test_from_github.py' to the cluster desktop
4. Install python 
5. In azure -> your cluster -> KDataMana comute resource -> identity -> User Managed Identity
5.1. open the installed identity 
5.2. Copy the object ID and Client ID to set_auth_env.bat
6. open a commandline window
7. run set_auth_env.bat
8. Run run_test_from_github.py with the following arguments 
8.1 "azure-kusto-python" (or your github repo name)
8.2 "[your branch name]" (or master)
8.3 "\tests\azure\auth_e2e.py"
8.4 "python" (or another runner if you are using a different SDK)

The script will download the repo from guthub.com and run the test you asked for.
In this case, MSI related tests will run.
Other tests require providing certificate info or App ID & Key info in the batch file
