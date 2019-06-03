from kit.backends import database

def test_from_sql():
    db_schema = database.from_sql('./data/sql/create_schema.sql')

    table = db_schema[0]



# def test_fromdict_partial():
#     dd = {"columnHeaders": False, "delimiter": "", "quoteStyle": "", "$type": ""}
#     file_format_settings = FileFormatSettings.fromdict(dd)
#
#     assert (file_format_settings.column_headers == dd['columnHeaders'])
#     assert (file_format_settings.delimiter == dd['delimiter'])
#     assert (file_format_settings.quote_style == dd['quoteStyle'])
#     assert (file_format_settings._type == dd['$type'])
#
#
# def test_fromdict_nested():
#     with open('../kit/examples/model.json') as f:
#         data = json.load(f)
#
#     model = Model.fromdict(data)
#
#     assert (model.name == 'OrdersProductsV3')
#     assert (model.version == '1.0')
#     assert (len(model.entities) == 2)
#     for entity in model.entities:
#         assert isinstance(entity, LocalEntity)
#
#     assert model.entities[0].name == 'Orders'
#     assert model.entities[1].name == 'Products'
