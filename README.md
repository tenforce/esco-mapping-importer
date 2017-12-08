Mapping Platform: Import Concepts Service
=========================================

Quickstart
----------

### Build the Docker Image

```
docker build -t import-concepts .
```

### Run the Container

```
docker run -it --rm -v $PWD:/app \
    --link some-db:db \
    -e SPARQL_ENDPOINT="http://db:8001/openrdf-sesame" \
    -e SPARQL_REPOSITORY=TEMP \
    -e SPARQL_USER=escordf -e SPARQL_PASSWORD=secret \
    -v /data \
    -p 80:9000 \
    import-concepts
```

### Warification

```
docker run -it --rm -v $PWD:/app import-concepts war
```

Database Configuration
----------------------

### Virtuoso

You need to provide the CRUD URL of Virtuoso:
```
SPARQL_CRUD_ENDPOINT=http://db:8890/sparql-graph-crud
```

### OWLIM

You need to provide the URL of the database, the repository and probably the
credentials:
```
SPARQL_ENDPOINT=http://db:8001/openrdf-sesame
SPARQL_REPOSITORY=TEMP
SPARQL_USER=escordf
SPARQL_PASSWORD=secret
```

### Change the Application Graph

```
docker run ... \
    -e GRAPH="http://custom/graph" \
    ...
```

Notes
-----

 *  This service is currently checking that the concept schemes do not already
    exist in the application graph before continuing. This is fine for now but
    we may want to move this check to the move-graph service for instance
    (and/or the validation).

Links
-----

 *  Project specifications on [Google Docs](https://docs.google.com/a/tenforce.com/document/d/1PjsSljn8C-Ym9By65gP6WezK-ajIhfFeqfdZn9Dnpjg/edit?usp=sharing).
 *  Sample ROME NOC file on [Google Docs](https://docs.google.com/a/tenforce.com/spreadsheets/d/1aUlcE2H9IjGrWt6nYHu8-RmU5dITePMeN3NFPnWXDW4/edit?usp=sharing).
 *  Example Mapping file on [Google Docs](https://docs.google.com/spreadsheets/d/1vZb4_Et5ZQvfUpDButG2a61jyV6JvXFcJVRE4vBRujw/edit#gid=0)
 *  Example Taxonomy file on [Google Docs](https://docs.google.com/a/tenforce.com/spreadsheets/d/1dRG4ebkLdGZwTuxfzXFylDV0oWYwY0nDw9rPOAC9LAA/edit?usp=sharing)
 *  Rome Classification example file on [Google Docs](https://docs.google.com/spreadsheets/d/1aUlcE2H9IjGrWt6nYHu8-RmU5dITePMeN3NFPnWXDW4/edit#gid=228629258)
