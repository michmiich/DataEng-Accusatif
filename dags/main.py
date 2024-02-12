import airflow
import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.postgres_operator import PostgresOperator
import urllib.request as request
from airflow.utils.task_group import TaskGroup
import redis
from redis.commands.json.path import Path
from pymongo import MongoClient
import requests
from time import sleep
import pyarrow as pa
from sqlalchemy import create_engine, Column, ForeignKey, Integer, String,Float,  DateTime
from sqlalchemy.orm import declarative_base, relationship, Session

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

main_dag = DAG(
    dag_id='main_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)


with TaskGroup("ingestion_pipeline","data ingestion step",dag=main_dag) as ingestion_pipeline:
    ingestion_start = DummyOperator(
        task_id='start',
        dag=main_dag,
    )

    def _check_connection():
        try:
            requests.get("https://google.com")
            print("connection tested")
        except requests.exceptions.ConnectionError:
            print("no connection")
            return [offline_source.task_id, ingestion_end.task_id]
        print("connection")
        return [online_source.task_id, ingestion_end.task_id]
            
    connection_check = BranchPythonOperator(
        task_id='connection_check',
        dag=main_dag,
        python_callable= _check_connection,
        op_kwargs={},
        trigger_rule='all_success',
    )

    online_source = EmptyOperator(
        task_id='online_source',
        dag=main_dag,
        trigger_rule='all_success'
    )

    offline_source = EmptyOperator(
        task_id='offline_source',
        dag=main_dag,
        trigger_rule='all_success'
    )

    def _download_imdb_datasets(epoch, url, output_folder):

        # Get the dataset from url
        # request.urlretrieve(url=f"{url}title.crew.tsv.gz", filename=f"{output_folder}/crew_{epoch}.tsv.gz")
        request.urlretrieve(url=f"{url}name.basics.tsv.gz", filename=f"{output_folder}/people_{epoch}.tsv.gz")
        request.urlretrieve(url=f"{url}title.ratings.tsv.gz", filename=f"{output_folder}/rates_{epoch}.tsv.gz")
        request.urlretrieve(url=f"{url}title.basics.tsv.gz", filename=f"{output_folder}/movie_{epoch}.tsv.gz")
        request.urlretrieve(url=f"{url}title.principals.tsv.gz", filename=f"{output_folder}/principals_{epoch}.tsv.gz")
        
    download_imdb_datasets = PythonOperator(
        task_id='download_imdb_datasets',
        dag=main_dag,
        python_callable=_download_imdb_datasets,
        op_kwargs={
            "output_folder": "/opt/airflow/dags/data",
            "epoch": "{{ execution_date.int_timestamp }}",
            "url": "http://datasets.imdbws.com/",
        },
        trigger_rule='all_success',
        depends_on_past=False,
    )

    def _store_datasets_in_mongoDB(epoch, output_folder, mongo_host, mongo_port,mongo_database, collections):

        # MongoDB client
        mongo_client = MongoClient(f"mongodb://{mongo_host}:{mongo_port}/")
        db = mongo_client[mongo_database]
        
        for collection in collections :
            print(collection)
            if(collection == "justice"):
                df = pd.read_csv(output_folder + "/" + collection +"_" + epoch + ".csv", nrows=1000000)
                #df = pd.read_csv(output_folder + "/" + collection +"_" + epoch + ".csv")
            else:
                # Unzip tsv to get panda Dataframe
                df = pd.read_csv(output_folder + "/" + collection +"_" + epoch + ".tsv.gz", compression="gzip", header=0, sep="\t", quotechar='"', nrows=1000000)
                #df = pd.read_csv(output_folder + "/" + collection +"_" + epoch + ".tsv.gz", compression="gzip", header=0, sep="\t", quotechar='"')
                #Rename id column to _id
                if(collection != "principals"):
                    df = df.rename(columns={ df.columns[0]: "_id" })
           
            print("read")
            print(df.head(10))
            for col in df.columns:
                print(col)
            
            # add a mongoDB collection
            col = db[collection]
            col.drop()
            col.insert_many(df.to_dict('records'))
            print("in mongo")

        print("inserted")
        print(db.list_collection_names())

        
    store_imdb_datasets = PythonOperator(
        task_id='store_imdb_datasets',
        dag=main_dag,
        python_callable=_store_datasets_in_mongoDB,
        op_kwargs={
            "output_folder": "/opt/airflow/dags/data",
            "epoch": "{{ execution_date.int_timestamp }}",
            "mongo_host": "mongo",
            "mongo_port": "27017",
            "mongo_database": "data",
            "collections":["people", "rates", "movie", "principals"]
        },
        trigger_rule='all_success',
        depends_on_past=False,
    )

    get_imdb_offline_backups = PythonOperator(
        task_id='get_imdb_offline_backups',
        dag=main_dag,
        python_callable=_store_datasets_in_mongoDB,
        op_kwargs={
            "output_folder": "/opt/airflow/dags/data/offline_samples",
            "epoch": "backup",
            "mongo_host": "mongo",
            "mongo_port": "27017",
            "mongo_database": "data",
            "collections":["people", "rates", "movie", "principals"]
        },
        trigger_rule='all_success',
        depends_on_past=False,
    )

    def _get_wikidata_dataset(endpoint_url, mongo_host, mongo_port,mongo_database):

        # DBPedia query to get infos of all the disstracks present on DBPedia
        sparql_query = """SELECT DISTINCT ?qqnLabel (GROUP_CONCAT(DISTINCT ?metierLabel; SEPARATOR = ",") AS ?metiers) ?raisonLabel WHERE {
            ?qqn p:P1399 [ps:P1399 ?raison].
            ?qqn p:P106 [(ps:P106) ?metier].
            ?metier p:P31 [(ps:P31) wd:Q4220920].
            SERVICE wikibase:label {
                bd:serviceParam wikibase:language "en,fr".
                ?qqn rdfs:label ?qqnLabel.
                ?metier rdfs:label ?metierLabel.
                ?raison rdfs:label ?raisonLabel.
            }
            }
            GROUP BY ?raisonLabel ?qqnLabel
            ORDER BY (?qqnLabel)
        """

        r = requests.get(
            f"{endpoint_url}", params={"format": "json", "query": sparql_query}
        )
        if not r.ok:
            # Probable too many requests, so sleep and retry
            sleep(1)
            r = requests.get(
                f"{endpoint_url}", params={"format": "json", "query": sparql_query}
            )

        print(r.encoding, "ENCODING AVANT")
        r.encoding = r.apparent_encoding
        print(r.encoding, "ENCODING APRES")
        offense = r.json()["results"]["bindings"]
        attributes = list(offense[0].keys())

        df_offense = pd.DataFrame(columns=attributes)
        print(df_offense)

        for idx, example in enumerate(offense):
            for attr in attributes:
                df_offense.loc[idx,attr] = example[attr]['value']

        print(df_offense.head(10))

        # Store in MongoDB client
        mongo_client = MongoClient(f"mongodb://{mongo_host}:{mongo_port}/")
        db = mongo_client[mongo_database]
        col = db["offense"]
        col.insert_many(df_offense.to_dict('records'))

    get_wikidata_dataset = PythonOperator(
        task_id='get_wikidata_dataset',
        dag=main_dag,
        python_callable=_get_wikidata_dataset,
        op_kwargs={
            "endpoint_url" : "https://query.wikidata.org/sparql",
            "mongo_host": "mongo",
            "mongo_port": "27017",
            "mongo_database": "data",
        },
        trigger_rule='all_success',
        depends_on_past=False,
    )

    get_wikidata_offline_backup = PythonOperator(
        task_id='get_wikidata_offline_backup',
        dag=main_dag,
        python_callable=_store_datasets_in_mongoDB,
        op_kwargs={
            "mongo_host": "mongo",
            "mongo_port": "27017",
            "mongo_database": "data",
            "output_folder": "/opt/airflow/dags/data/offline_samples",
            "epoch": "backup",
            "collections":["justice"]
        },
        trigger_rule='all_success',
        depends_on_past=False,
    )

    ingestion_end = DummyOperator(
        task_id='end',
        dag=main_dag,
        trigger_rule='none_failed'
    )

    ingestion_start >> connection_check >> [online_source, offline_source]

    online_source >> [download_imdb_datasets, get_wikidata_dataset]
    download_imdb_datasets >> store_imdb_datasets >> ingestion_end
    get_wikidata_dataset >> ingestion_end

    offline_source >> [get_imdb_offline_backups, get_wikidata_offline_backup] >> ingestion_end
    
with TaskGroup("staging_pipeline","data staging step",dag=main_dag) as staging_pipeline:
    stagin_start = DummyOperator(
        task_id='start',
        dag=main_dag,
    )

    def _store_redis(mongo_host, mongo_port,mongo_database, collections):
        mongo_client = MongoClient(f"mongodb://{mongo_host}:{mongo_port}/")
        db = mongo_client[mongo_database]

        redis_client = redis.Redis(host="rejson", port=6379, db=0)
        context = pa.default_serialization_context()

        for col in collections:
            print("--------",col,"-----------")
            data = db[col]
            print("là c'est ok")
            df = pd.DataFrame(list(data.find()))
            print("là aussi")
            print(df.head(10))
            #print(df.head(10).to_json(orient = 'records'))
            df["_id"] = df["_id"].str.encode(encoding='utf-8')
            print(df.head(10).to_json(orient = 'records'))
            redis_client.set(col, context.serialize(df).to_buffer().to_pybytes())
            print("OUIIIIII")
            # storing in redis
            #redis_client.json().set(col, Path.root_path(), df.to_json(orient = 'records'))

    store_redis = PythonOperator(
        task_id='store_redis',
        dag=main_dag,
        python_callable=_store_redis,
        op_kwargs={
            "mongo_host": "mongo",
            "mongo_port": "27017",
            "mongo_database": "data",
            "collections":["people", "rates", "movie", "principals", "offense"]
        },
        trigger_rule='all_success',
        depends_on_past=False,
    )

    def _remove_columns(redis_host, redis_port,redis_database):
        redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_database)

        context = pa.default_serialization_context()
        
        people = context.deserialize(redis_client.get("people"))
        movie = context.deserialize(redis_client.get("movie"))
        principals = context.deserialize(redis_client.get("principals"))
        offense = context.deserialize(redis_client.get("offense"))

        df_movie = pd.DataFrame(movie)
        df_people = pd.DataFrame(people)
        df_principals = pd.DataFrame(principals)
        df_offense = pd.DataFrame(offense)

        df_people= df_people.drop(columns=['deathYear', 'knownForTitles'], errors = "ignore")
        df_movie = df_movie.drop(columns=['originalTitle', 'isAdult', 'endYear', 'runtimeMinutes'], errors = "ignore")
        df_principals = df_principals.drop(columns=['characters', 'job'], errors = "ignore")
        df_offense = df_offense.drop(columns=['metiers', '_id'], errors = "ignore")

        print(df_people.head(10))
        print(df_movie.head(10))
        print(df_principals.head(10))
        print(df_offense.head(10))

        redis_client.set("people", context.serialize(df_people).to_buffer().to_pybytes())
        redis_client.set("movie", context.serialize(df_movie).to_buffer().to_pybytes())
        redis_client.set("principals", context.serialize(df_principals).to_buffer().to_pybytes())
        redis_client.set("offense", context.serialize(df_offense).to_buffer().to_pybytes())
    
        """df_people = pd.read_csv(output_folder + "/people_" + epoch + ".tsv.gz", compression="gzip", header=0, sep="\t", quotechar='"', nrows=10000)
        df_rates = pd.read_csv(output_folder + "/rates_" + epoch + ".tsv.gz", compression="gzip", header=0, sep="\t", quotechar='"', nrows=10000)
        df_movie = pd.read_csv(output_folder + "/movie_" + epoch + ".tsv.gz", compression="gzip", header=0, sep="\t", quotechar='"', nrows=10000)
        df_principals = pd.read_csv(output_folder + "/principals_" + epoch + ".tsv.gz", compression="gzip", header=0, sep="\t", quotechar='"', nrows=10000)

        # Drop unuseful columns
        df_people= df_people.drop(columns=['deathYear', 'knownForTitles'])
        df_movie = df_movie.drop(columns=['originalTitle', 'isAdult', 'endYear', 'runtimeMinutes'])
        df_principals = df_principals.drop(columns=['characters'])

        print("read")
        for col in df_people.columns:
            print(col)
        print(df_people.head(10))

        for col in df_rates.columns:
            print(col)
        print(df_rates.head(10))

        for col in df_movie.columns:
            print(col)
        print(df_movie.head(10))

        for col in df_principals.columns:
            print(col)
        print(df_principals.head(10))

        # Save the result to redis db (to speed up the steps as it uses cache)
        client = redis.Redis(host="rejson", port=6379, db=0)

        client.json().set("people", Path.root_path(), df_people.to_json(orient = 'records'))
        client.json().set("movie", Path.root_path(), df_movie.to_json(orient = 'records'))
        client.json().set("principals", Path.root_path(), df_principals.to_json(orient = 'records'))
        client.json().set("rate", Path.root_path(), df_rates.to_json(orient = 'records'))"""

    remove_columns = PythonOperator(
        task_id='remove_columns',
        dag=main_dag,
        python_callable=_remove_columns,
        op_kwargs={
            "redis_host": "rejson",
            "redis_port": 6379,
            "redis_database": 0,
        },
        trigger_rule='all_success',
        depends_on_past=False,
    )

    def _join_movies_with_rates():

        redis_client = redis.Redis(host="rejson", port=6379, db=0)
        context = pa.default_serialization_context()
        movie = context.deserialize(redis_client.get("movie"))
        rate = context.deserialize(redis_client.get("rates"))
        #movie = client.json().get("movie")
        #df_movie = pd.read_json(movie)
        
        df_movie = pd.DataFrame(movie)
        df_rate = pd.DataFrame(rate)

        print(df_movie.head(10))
        print(df_rate.head(10))
        
        df_movie = df_movie.set_index('_id').join(df_rate.set_index('_id'))
        for col in df_movie.columns:
            print(col)
        print(df_movie.dtypes)

        redis_client.set("movie", context.serialize(df_movie).to_buffer().to_pybytes())

    join_movies_with_rates = PythonOperator(
        task_id='join_movies_with_rates',
        dag=main_dag,
        python_callable=_join_movies_with_rates,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )

    def _rename_columns():

        redis_client = redis.Redis(host="rejson", port=6379, db=0)
        context = pa.default_serialization_context()

        # offense
        offense = context.deserialize(redis_client.get("offense"))
        df_offense = pd.DataFrame(offense)
        df_offense = df_offense.rename(columns={"qqnLabel": "suspectName", "raisonLabel": "reason"})
        print(df_offense.head(10))

        # principals
        principals = context.deserialize(redis_client.get("principals"))
        df_principals = pd.DataFrame(principals)
        df_principals = df_principals.rename(columns={"tconst": "event_id", "nconst": "people_id", "category": "role"})
        print(df_principals.head(10))

        redis_client.set("offense", context.serialize(df_offense).to_buffer().to_pybytes())
        redis_client.set("principals", context.serialize(df_principals).to_buffer().to_pybytes())

    rename_columns = PythonOperator(
        task_id='rename_columns',
        dag=main_dag,
        python_callable=_rename_columns,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )

    def _merge_principals_with_offense():

        redis_client = redis.Redis(host="rejson", port=6379, db=0)
        context = pa.default_serialization_context()
        principals = context.deserialize(redis_client.get("principals"))
        offense = context.deserialize(redis_client.get("offense"))
        people = context.deserialize(redis_client.get("people"))
        
        df_principals = pd.DataFrame(principals)
        df_offense = pd.DataFrame(offense)
        df_people = pd.DataFrame(people)

        
        print(df_principals.head(10))
        print(df_offense.head(10))
        
        print(len(df_offense))
        df_offense = pd.merge(df_offense, df_people[['_id', 'primaryName']], how="left", left_on='suspectName', right_on='primaryName').rename(columns = {'_id':"people_id" })
        df_offense["event_type"] = "offense"
        df_principals["event_type"] = "movie"
        df_fact = pd.concat([df_principals[["people_id", "event_id", "role", "event_type"]], df_offense.reset_index(names = "event_id")[["people_id", "event_id", "event_type"]]],ignore_index = True)
        
        print(df_fact.head(50))
        print(df_fact.dtypes)
        #df_fact = pd.merge(df_principals, df_offense, how="outer", left_on='people_id', right_on='suspectName')
        for col in df_fact.columns:
            print(col)
            print(df_fact.loc[1,col])
        print(len(df_fact))
        print(df_fact.dtypes)
        print(df_principals.dtypes)
        print(df_offense.dtypes)
        print(df_people.dtypes)
        redis_client.set("offense", context.serialize(df_offense).to_buffer().to_pybytes())
        redis_client.set("fact", context.serialize(df_fact).to_buffer().to_pybytes())

    merge_principals_with_offense = PythonOperator(
        task_id='merge_principals_with_offense',
        dag=main_dag,
        python_callable=_merge_principals_with_offense,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )

    staging_end = DummyOperator(
        task_id='staging_end',
        dag=main_dag,
        trigger_rule='all_success'
    )

    stagin_start >> store_redis >> remove_columns >> join_movies_with_rates >> rename_columns >> merge_principals_with_offense >> staging_end

with TaskGroup("production_pipeline","data production step",dag=main_dag) as production_pipeline:
    production_start = DummyOperator(
        task_id='start',
        dag=main_dag,
    )

    def _saving_to_postgres(
    redis_host: str,
    redis_port: int,
    redis_db: int,
    postgres_host: str,
    postgres_port: int,
    postgres_db: str,
    postgres_user: str,
    postgres_pswd: str,
    table= str
    ):

        Base = declarative_base()
        
        class Event(Base):
            __tablename__ = "event"
            id = Column(Integer, primary_key=True)
            event_type = Column(String)
            __mapper_args__ = {
                "polymorphic_identity": "event",
                "polymorphic_on": "event_type",
            }

        class People(Base):
            __tablename__ = "people"
            id = Column(Integer,  primary_key=True)
            primaryProfession = Column(String)
            primaryName = Column(String)
            birthYear = Column(String)
        
            def __repr__(self):
                return f"Entity(id={self.id!r}, primaryName={self.primaryName!r}, primaryProfession={self.primaryProfession!r},  birthYear={self.birthYear!r})"
        
            
        class Crime(Event):
            __tablename__ = "crime"
            id = Column(Integer, ForeignKey("event.id"), primary_key=True)
            suspectName = Column(String)
            reason = Column(String)
            __mapper_args__ = {
                "polymorphic_identity": "crime",
            }
        
            def __repr__(self):
                return f"Entity(id={self.id!r}, suspectName={self.suspectName!r}, reason={self.reason!r})"
        
        class Movie(Event):
            __tablename__ = "movie"
            id = Column(Integer, ForeignKey("event.id"), primary_key=True)
            titleType = Column(String, nullable=False)
            primaryTitle = Column(String, nullable=False)
            startYear = Column(String)
            genres = Column(String)
            averageRating = Column(Float)
            numVotes = Column(Float)
            __mapper_args__ = {
                "polymorphic_identity": "movie",
            }
            
            def __repr__(self):
                return f"movie(id={self.id!r}, primaryTitle={self.primaryTitle!r}, startYear={self.startYear!r})"
        
        class Facts(Base):
            __tablename__ = "facts"
            id = Column(Integer, primary_key=True)
            role = Column(String)
            event_type = Column(String)
            people_id = Column(Integer, ForeignKey("people.id"), nullable=False)
            event_id = Column(Integer, ForeignKey("event.id"), nullable=False)
        
            people = relationship(
                "People", backref="had a role in", foreign_keys=[people_id]
            )
            event = relationship(
                "Event", backref="was done by", foreign_keys=[event_id]
            )
        
        
            def __repr__(self):
                return f"Entity(id={self.id!r}, role={self.role!r}, event_type={self.event_type!r},  people_id={self.people_id!r}, event_id={self.event_id!r})"
        
        engine = create_engine(
            f"postgresql://{postgres_user}:{postgres_pswd}@{postgres_host}:{postgres_port}/{postgres_db}"
        )
    
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        context = pa.default_serialization_context()
        
        if table == "movie":
            movie = context.deserialize(redis_client.get("movie"))
            df_movie = pd.DataFrame(movie)
            df_movie["event_type"] = "movie"
            with Session(engine) as session:
                for row in df_movie.iterrows():
                    titleType = row[1]["titleType"]
                    primaryTitle = row[1]["primaryTitle"]
                    startYear = row[1]["startYear"] # TODO : try/catch to 0 si pas int
                    numVotes = float(row[1]["numVotes"])
                    averageRating = float(row[1]["averageRating"])
                    genres = row[1]["genres"]
                    movie_obj = Movie(titleType = titleType, primaryTitle = primaryTitle, startYear = startYear, genres = genres, averageRating = averageRating, numVotes = numVotes)
                    session.add(movie_obj)

        elif table == "offense":
            offense = context.deserialize(redis_client.get("offense"))
            df_offense = pd.DataFrame(offense)
            with Session(engine) as session:
                for row in df_offense.iterrows():
                    suspectName = row[1]["suspectName"]
                    reason = row[1]["reason"]
                    offense_obj = Crime(suspectName = suspectName, reason = reason)
                    session.add(offense_obj)

        elif table == "people":
            people = context.deserialize(redis_client.get("people"))
            df_people = pd.DataFrame(people)
            with Session(engine) as session:
                for row in df_people.iterrows():
                    primaryProfession = row[1]["primaryProfession"]
                    primaryName = row[1]["primaryName"]
                    birthYear = row[1]["birthYear"]# TODO : try/catch to 0 si pas int
                    people_obj = People(primaryProfession=primaryProfession, primaryName = primaryName, birthYear=birthYear)              
                    session.add(people_obj)

        elif table ==  "fact":
            fact = context.deserialize(redis_client.get("fact"))
            df_fact = pd.DataFrame(fact)
            with Session(engine) as session:
                for row in df_fact.iterrows():
                    role = row[1]["role"]
                    event_type = row[1]["event_type"]
                    people_id = row[1]["people_id"]
                    event_id = row[1]["event_id"]
                    fact_obj = Fact(role=role, event_type = event_type, people_id=people_id,event_id = event_id)                 
                    session.add(fact_obj)
            session.commit()

    saving_node_movie = PythonOperator(
        task_id="saving_movie_to_postgres",
        dag=main_dag,
        trigger_rule="none_failed",
        python_callable=_saving_to_postgres,
        op_kwargs={
            "redis_host": "rejson",
            "redis_port": 6379,
            "redis_db": 0,
            "postgres_host": "postgres",
            "postgres_port": 5432,
            "postgres_db": "airflow",
            "postgres_user": "airflow",
            "postgres_pswd": "airflow",
            "table": "movie"
        },
    )

    saving_node_offense = PythonOperator(
        task_id="saving_offense_to_postgres",
        dag=main_dag,
        trigger_rule="none_failed",
        python_callable=_saving_to_postgres,
        op_kwargs={
            "redis_host": "rejson",
            "redis_port": 6379,
            "redis_db": 0,
            "postgres_host": "postgres",
            "postgres_port": 5432,
            "postgres_db": "airflow",
            "postgres_user": "airflow",
            "postgres_pswd": "airflow",
            "table": "offense"
        },
    )

    saving_node_people = PythonOperator(
        task_id="saving_people_to_postgres",
        dag=main_dag,
        trigger_rule="none_failed",
        python_callable=_saving_to_postgres,
        op_kwargs={
            "redis_host": "rejson",
            "redis_port": 6379,
            "redis_db": 0,
            "postgres_host": "postgres",
            "postgres_port": 5432,
            "postgres_db": "airflow",
            "postgres_user": "airflow",
            "postgres_pswd": "airflow",
            "table": "people"
        },
    )

    saving_node_fact = PythonOperator(
        task_id="saving_fact_to_postgres",
        dag=main_dag,
        trigger_rule="none_failed",
        python_callable=_saving_to_postgres,
        op_kwargs={
            "redis_host": "rejson",
            "redis_port": 6379,
            "redis_db": 0,
            "postgres_host": "postgres",
            "postgres_port": 5432,
            "postgres_db": "airflow",
            "postgres_user": "airflow",
            "postgres_pswd": "airflow",
            "table": "fact"
        },
    )

    def _saving_to_neo4j(
    pg_user: str,
    pg_pwd: str,
    pg_host: str,
    pg_port: str,
    pg_db: str,
    neo_host: str,
    neo_port: str,
    ):

        query = """
                    SELECT *
                    FROM event e 
                    LEFT JOIN movie m
                    ON e.event_id = m.id
                    LEFT JOIN crime c 
                    ON e.event_id = c.id
                    LEFT JOIN people p
                    ON e.people_id = p.id
                """
        
        engine = create_engine(
            f'postgresql://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{pg_db}'
        )
        df = pd.read_sql(query, con=engine)
        print(df.columns.values)
        engine.dispose()

        graph = Graph(f"bolt://{neo_host}:{neo_port}")

        graph.delete_all()
        tx = graph.begin()
        for _, row in df.iterrows():
            if row["e.event_type"] == "movie":
                tx.evaluate('''
                MERGE (p:person {people_id:$people_id, name:$peopleName, year:$birthYear, profession:$primaryProfession})
                MERGE (m:movie {movie_id:$movie_id, titleType:$titleType, primaryTitle:$primaryTitle, year:$startYear, genre:$genres, rating:$averageRating, num:$numVotes})
                MERGE (p)-[r:worked_on]->(m)
                ''', parameters = {'people_id': row['p.id'],
                 'peopleName': row['p.peopleName'], 
                 'birthYear': int(row['p.birthYear']),
                  'primaryProfession': row['p.primaryProfession'],
                  'movie_id': row['m.id'],
                 'titleType': row['m.titleType'],
                 'primaryTitle': row['m.primaryTitle'], 
                 'startYear': int(row['m.startYear']),
                  'genres': row['m.genres'],
                  'averageRating': float(row['m.averageRating']), 
                 'numVotes': int(row['m.numVotes'])})
            elif row["e.event_type" == "offense"]:
                tx.evaluate('''
                MERGE (p:person {people_id:$people_id, name:$peopleName, year:$birthYear, profession:$primaryProfession})
                MERGE (c:crime {crime_id:$crime_id, reason:$reason})
                MERGE (p)-[r:commited]->(c)
                ''', parameters = {'people_id': row['p.id'],
                 'peopleName': row['p.peopleName'], 
                 'birthYear': int(row['p.birthYear']),
                  'primaryProfession': row['p.primaryProfession'],
                  'crime_id': row['c.id'],
                 'reason': row['c.reason']})
        tx.commit()

    graph_saving_node = PythonOperator(
        task_id="saving_to_neo4j",
        dag=main_dag,
        trigger_rule="all_success",
        python_callable=_saving_to_neo4j,
        op_kwargs={
            "pg_user": "airflow",
            "pg_pwd": "airflow",
            "pg_host": "postgres",
            "pg_port": "5432",
            "pg_db": "postgres",
            "neo_host": "neo4j",
            "neo_port": "7687",
        },
    )

    production_stop = DummyOperator(
        task_id='stop',
        dag=main_dag,
    )

    production_start >> [saving_node_movie, saving_node_offense, saving_node_people] >> saving_node_fact >> graph_saving_node>>production_stop


start_global = DummyOperator(
    task_id='start_global',
    dag=main_dag,
    trigger_rule='all_success'
)

end_global = DummyOperator(
    task_id='end_global',
    dag=main_dag,
    trigger_rule='all_success'
)



start_global >> ingestion_pipeline >> staging_pipeline >> production_pipeline >> end_global