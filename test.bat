cd %PROJECTS_HOME%\azure-kusto-python
call workon kusto
call pip uninstall azure-kusto-data azure-kusto-ingest -y 
call pip install ./azure-kusto-data[pandas] ./azure-kusto-ingest[pandas] 
call pip install --force-reinstall azure-nspkg==1.0.0 
call pytest
pause