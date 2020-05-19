# SQLMake: data pipeline tool for SQL

SQLMake is a data pipeline tool for SQL. On many projects I have a bunch of manipulations of data I want to do in an SQL database. In the past I would just have a bunch of SQL scripts, but that becomes unwieldy fast. Since the scripts are often long-running (some queries take hours), I don't want to re-run the entire pipeline when I make a change in one place. Most of the scripts either create new tables, or add columns to existing tables.

SQLMake allows you to codify the relationships between those scripts in a meaningful way, and documents what their outputs are. Running the `sqm` command then examines the state of the database, and runs only those tasks needed to bring the database up to date. To reproduce the whole database, just drop the database and run `sqm`---every query will automatically be re-run.

## Configuration

SQMake uses a yaml-based configuration file, which by default is searches for at ./sqmake.yml. Here's the format of that file:

```yaml
# SQLAlchemy database connection string
db: postgresql://localhost:5432/matthewc

# Schema to look for process output tables
schema: diss

# Tasks to be run in the database
tasks:
    # First task: download data using an sh-command
  - name: download-osm
    commands:
        # each command has a type, which can be sh or sql, for where that command should be run
      - type: sh
        # each command can either have code or file. Code will be directly executed, while file will be read and executed.
        code: curl -o "/Users/matthewc/osm/socal_osm/socal.osm.pbf" https://download.geofabrik.de/north-america/us/california/socal-latest.osm.pbf
    outputs:
        # outputs can be files, tables, or columns
        # if outputs already exist, task will not be run
      - file: /Users/matthewc/osm/socal_osm/socal.osm.pbf

    # second task: load data
  - name: load-osm-to-database
    commands:
      # set up the schema
      - type: sql
        file: pgsnapshot_schema_0.6.sql
      # load the data
      - type: sh
        code: osmosis --read-pbf "/Volumes/Pheasant Ridge/osm/socal_osm/socal.osm.pbf" --write-pgsql database=matthewc postgresSchema=diss
    depends_on: download-osm
    outputs:
      - table: diss.users
      - table: diss.nodes
      - table: diss.ways
      - table: diss.way_nodes
      - table: diss.relations
      - table: diss.relation_members
      - table: diss.schema_info

# Other sqmake files to include, with a namespace for their tasks (tasks can be referred to as namespace/task)
includes:
  - name: setbacks
    file: setbacks/sqmake.yml
```

## Running

Running `sqm task` will run `task`, after making sure all dependencies are satisfied.