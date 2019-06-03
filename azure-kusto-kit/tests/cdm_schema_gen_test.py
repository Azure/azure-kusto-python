import json

from kit.models.cdm import Model
from kit.backends import table, database


def test_table_from_entity():
    with open('./cdm/data/model.json') as f:
        data = json.load(f)

    model = Model.fromdict(data)
    entity = model.entities[0]
    table_schema = table.from_cdm_entity(entity)

    assert (table_schema.table_name == entity.name)
    assert (len(table_schema.columns) == len(entity.attributes))
    for i, c in enumerate(table_schema.columns):
        assert (c.target_type == entity.attributes[i]._type)
        assert (c.target_column == entity.attributes[i].name)


def test_database_from_model():
    with open('./cdm/data/model.json') as f:
        data = json.load(f)

    model = Model.fromdict(data)
    db_schema = database.from_cdm_model(model)

    assert (len(db_schema) == 2)
