from kit.core.ingestion import FolderIngestionFlow

#
# def test_ingest_dry_run_flat_folder():
#     folder_flow = FolderIngestionFlow('./data/raw/flat/imdb', 'https://dummy.kusto.windows.net', target_db="testdb", dry=True)
#     summary = folder_flow.run()
#
#     assert summary['tables_affected'] == 5
#     assert summary['operations'] == 5
#
#     folder_flow.kusto_backend.client_provider.get_engine_client().execute('')

def test_ingest_dry_run_nested_folder():
    folder = FolderIngestionFlow('./data/raw/nested/imdb', 'https://dummy.kusto.windows.net', target_db="testdb", dry=True)
    summary = folder.run()