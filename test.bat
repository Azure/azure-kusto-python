cd %PROJECTS_HOME%\azure-kusto-python
call pip uninstall azure-kusto-data azure-kusto-ingest -y
call pip install ./azure-kusto-data[pandas] ./azure-kusto-ingest[pandas] 
call pip install --force-reinstall azure-nspkg==2.0.0 
call pytest
pause