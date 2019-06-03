import logging

import click

logging.basicConfig(format='%(asctime)s | %(levelname)s | %(filename)s | %(message)s', level=logging.ERROR)

logger = logging.getLogger('kit')
logger.setLevel(logging.INFO)


def auth_from_cli(app, user, host):
    from kit.helpers import get_azure_cli_auth_token
    if app:
        aad_app_id, app_key = app.split(':')
        return dict(aad_app_id=aad_app_id, app_key=app_key, authority_id='72f988bf-86f1-41af-91ab-2d7cd011db47')
    if user:
        user_id, password = user.split(':')
        return dict(user_id=user_id, password=password, authority_id='72f988bf-86f1-41af-91ab-2d7cd011db47')

    return dict(user_token=get_azure_cli_auth_token(host))


@click.group()
@click.pass_context
def main(ctx):
    pass


@main.command()
@click.option('--user', type=str)
@click.option('--app', type=str)
@click.option('--dry', is_flag=True, default=False)
@click.option('--nowait', is_flag=True, default=False)
@click.option('--queued', '-q', is_flag=True, default=False)
@click.option('--host', '-h', type=str)
@click.option('--directory', '-d', type=click.Path())
@click.pass_context
def ingest(ctx, directory, host, queued, nowait, dry, app, user):
    from kit.core.ingestion import FolderIngestionFlow
    flow = FolderIngestionFlow(directory, host, dry=dry, auth=auth_from_cli(app, user, host), queued=queued, no_wait=nowait)
    flow.run()


@main.group()
@click.pass_context
def schema(ctx):
    pass


@schema.command()
@click.pass_context
@click.option('--user', type=str)
@click.option('--app', type=str)
@click.option('--sql', '-sql', type=str)
@click.option('--host', '-h', type=str)
@click.option('--database', '-db', type=str)
@click.option('--directory', '-d', type=click.Path())
@click.option('--file', '-f', type=click.Path())
def create(ctx, file, directory, database, host, sql, app, user):
    if file:
        # TODO: should resolve file location (can be s3 / azure storage / local file)
        from kit.backends import fs
        from kit.models.database import Database
        table = fs.table_from_file(file)

        db = Database('temp', [table])

    elif directory:
        from kit.backends import fs
        from kit.models.database import Database
        db = fs.database_from_folder(directory)
    elif host:
        auth = auth_from_cli(app, user, host)
        from kit.backends.kusto import KustoBackend
        kusto_backend = KustoBackend(host, auth)
        db = kusto_backend.describe_database(database)
    elif sql:
        import kit.specifications.sql as sql_spec
        db = sql_spec.database_from_sql(sql)
    else:
        raise ValueError("No method was chosen. use `kit schema create --help` ")

    print(db.to_json())


@schema.command()
@click.pass_context
def sync(ctx):
    pass


if __name__ == '__main__':
    main()
