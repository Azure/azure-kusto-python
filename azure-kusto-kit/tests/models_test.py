import json

from kit.models import Database


def test_database_from_file():
    with open('./tests/data/kusto/database.json') as f:
        json_obj = json.load(f)
        db = Database.fromdict(json_obj)

        assert db.name == json_obj['name']
        assert len(db.tables) == len(json_obj['tables'])
        assert db.tables_lookup['aka_titile'].columns == json_obj['tables']
