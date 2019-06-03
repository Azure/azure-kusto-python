# Kusto Ingestion Tools (Kit)

## Purpose
Make ingestion simpler (at least for common cases)
After creating an ADX cluster via Azure, we want to explore some data.
It is a common use case while evaluating data sources or just POC-ing capabitlites we want to move fast.
That is why this project exist. It contains several common case tools to help with first steps of **schema creation** and **ingestion**.

## Ideas
Given a data source, usually the workflow would consist of:

1. Describing the data source.
2. Preparing the target data store (in our case, Kusto)
3. Mapping Source to Target
4. Loading the data
5. *Optional* : Automation / Moving to Production

## Usage

### Basic

`kit ingest -d /path/to/data/imdb -h mycluster.westus`

The following command will try to ingest all files in `/path/to/data/imdb` (non-recursive) using type inference.


**<!>NOTICE<!>**: without providing any other arguments, this command is extremely *opinionated*, and will assume the following:

### Example 1 : Join Order Benchmark

One useful scenario would be to load an entire existing dataset into Kusto.
Let's take for example the [Join Order Benchmark](https://github.com/gregrahn/join-order-benchmark) used in the paper [How good are query optimizers really?](http://www.vldb.org/pvldb/vol9/p204-leis.pdf).

### Example 2: CDM

`kit ingest --metadata /path/to/cdmmodel.json --meatatype cdm`

### Example 2: Apache Hive MetaStore

`kit ingest --metadata /path/to/cdmmodel.json --meatatype cdm`


## Defaults

### Auth
By default, will try to grab token from [azure cli](https://docs.microsoft.com/en-us/cli/azure/?view=azure-cli-latest)

### Naming
* **database** will be set to is the dir in which data sits, so `/path/to/data/imdb` will look for, and create if missing, a database named `imdb`. 
If more control is need, try `--database`
* **tables** are actual file names, so `/path/to/data/imdb/aka_name.csv` will look for, and create if missing, a table named `aka_name`. 
This can be tweaked by making sure data is split into folder, where any folder would be a table.
This recursive mode assumes that the table structure is the same for all files.  

### Conflicting Files
* Safe `--data-conflict=safe` - Safest option is providing conflict resolution flag, which will bail in case if any conflicts (default) 

Below are not implemented yet 
* Extend `--data-conflict=extend` - In any case of conflicts, kit will try and extend the scheme as much as possible to accommodate for change 
* Merge `--data-conflict=merge` - In case you want to be more aggressive, merge can be used, which will do it's best to follow your wishes (in case of conflicts, will try to auto-resolve)

### Schema Conflicts
* Safe `--schema-conflict=safe` - Safest option is providing conflict resolution flag, which will bail in case if any conflicts (default) 

Below are not implemented yet:
* Append `--schema-conflict=append` - In any case of conflicts, kit will create copies (if table exists but schema is mismatched, will create a new table with same name + '_') 
* Merge `--schema-conflict=merge` - In case you want to be more aggressive, merge can be used, which will do it's best to follow your wishes (in case of conflicts, will try to auto-resolve)

## Files

### Database file
This is a simple way to describe a database.

This can be used to describe a db schema using plain JSON format, and thus easily copy entire database schemas.

```json
{
  "name": "imdb",
  "tables": [
    {
      "name": "aka_name",
      "columns": [
        {
          "name": "id",
          "data_type": "str"
        }
      ]
    },
    {
      "name": "aka_title",
      "columns": [
        {
          "name": "id",
          "data_type": "str"
        },
        {
          "name": "title",
          "data_type": "str"
        }
      ]
    }
  ]
}
```

#### Creation Methods:

**Manually** 

`kit schema create --empty > schema.json`
    
**From an existing cluster**

`kit schema create -h 'https://mycluster.kusto.windows.net' -u appkey/userid -p appsecret/password -a authprovider > schema.json`

**From an sql file**

`kit schema create -sql create_statements.sql > schema.json`

**From a folder with raw data**

`kit schema create -d path/to/dir > schema.json`

**More to come...**


#### Sync 

Once we have a database file, we can generate the entire scheme using

`kit schema sync -f schema.json`

### Manifest file
A file to describe the details of an ingestion which can be run later

```json
{
  "databases": [
    "same as schema.json"
  ],
  "mappings": [
    {
      "name": "aka_name_from_csv",
      "columns": [
        {
          "source": {
            "index": 0,
            "data_type": "str"
          },
          "target": {
            "index": 0,
            "data_type": "str"
          }
        }
      ]
    }
  ],
  "operations": [
    {
      "database": "imdb",
      "sources": [
        {
          "files": [
            "1.csv",
            "...",
            "99.csv"            
          ],
          "mapping": "aka_name_from_csv"
        }
      ],
      "target": [
        "aka_name"
      ]
    }
  ]
}
```

#### Generate 

This file can be generated before ingestion operation to describe what is going to happen during ingestion


**Manually**

`kit ingestion manifest --db database.json `

**
