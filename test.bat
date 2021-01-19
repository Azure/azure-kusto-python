cd %PROJECTS_HOME%\azure-kusto-python
call workon kusto
call pip uninstall azure-kusto-data azure-kusto-ingest azure-kusto-logging -y 
call pip install ./azure-kusto-data[pandas] ./azure-kusto-ingest[pandas]  ./azure-kusto-logging
call pip install --force-reinstall azure-nspkg==2.0.0 
call pytest
pause