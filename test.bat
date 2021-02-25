cd %PROJECTS_HOME%\azure-kusto-python
call workon kusto
call pip uninstall azure-kusto-data azure-kusto-ingest -y 
call pip install -e ./azure-kusto-data[pandas]
call pip install -e ./azure-kusto-ingest[pandas]
call pip install --force-reinstall azure-nspkg==2.0.0 
call pytest
pause