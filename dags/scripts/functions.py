import logging
import requests
import numpy as np
import pandas as pd
from functools import reduce, partial

def download_datasets():
    """Download the datasets and store them in dags/files"""

    base_url = "https://datasets.imdbws.com/"
    files = [
        "title.basics.tsv.gz",
        "title.ratings.tsv.gz",
        "title.crew.tsv.gz"
    ]
    for file in files :
        r = requests.get(f"{base_url}{file}")
        with open(f"/home/airflow/dags/files/{file}", "wb") as outfile:
            outfile.write(r.content)
            logging.info(f"{file} successfully downloaded!")

def process_datasets():
    """Read the datasets and compute the metrics"""

    logging.info("Reading datasets...")
    df_basics = pd.read_csv(
        "/home/airflow/dags/files/title.basics.tsv.gz", 
        sep="\t", dtype=str, engine="c", 
        usecols=["tconst", "titleType", "startYear", "runtimeMinutes", "genres"],
        compression="gzip")
    df_ratings = pd.read_csv(
        "/home/airflow/dags/files/title.ratings.tsv.gz", 
        sep="\t", engine="c", compression="gzip")
    df_crew = pd.read_csv(
        "/home/airflow/dags/files/title.crew.tsv.gz", 
        sep="\t", dtype=str, engine="c", compression="gzip")

    # Filter and clean the data
    df_basics = df_basics.replace("\\N", np.nan)
    df_basics["startYear"] = pd.to_numeric(df_basics["startYear"], errors="coerce")
    df_basics["runtimeMinutes"] = pd.to_numeric(df_basics["runtimeMinutes"], errors="coerce")
    df_basics = df_basics[(df_basics["titleType"]=="movie") & (df_basics["startYear"].between(2015, 2020))]
    df_basics = df_basics.drop(columns=["titleType"]).reset_index(drop=True)

    df_crew = df_crew.replace("\\N", np.nan)

    # Merge the datasets
    merge = partial(pd.merge, on="tconst", how='left')
    df = reduce(merge, [df_basics, df_ratings, df_crew])

    logging.info("Computing metrics...")
    try :
        df["genres"] = df["genres"].str.split(",")
        df = df.explode('genres').reset_index(drop=True).dropna(subset=["genres"])

        # Compute avg runtimeMinutes, avg averageRating and total numVotes
        cols = ["startYear", "genres", "runtimeMinutes", "averageRating", "numVotes"]
        df1 = df[cols].groupby(["startYear", "genres"]) \
                    .agg({"runtimeMinutes": "mean", "averageRating": "mean", "numVotes": "sum"}) \
                    .reset_index()

        # Compute numDirectors
        df_tmp = df[["startYear", "genres", "directors"]].copy()
        df_tmp["directors"] = df_tmp["directors"].str.split(",")
        df_tmp = df_tmp.explode('directors').reset_index(drop=True)
        df2 = df_tmp.groupby(["startYear", "genres"])["directors"].nunique().reset_index()
        df2.rename(columns={"directors":"numDirectors"}, inplace=True)

        # Compute topDirectors
        df3 = df_tmp.groupby(['startYear', 'genres']) \
                    .agg(lambda x : "; ".join(list(x.mode()))).reset_index()

        df3.rename(columns={"directors": "topDirectors"}, inplace=True)

        # Compute numWriters
        df_tmp = df[["startYear", "genres", "writers"]].copy()
        df_tmp["writers"] = df_tmp["writers"].str.split(",")
        df_tmp = df_tmp.explode('writers').reset_index(drop=True)
        df4 = df_tmp.groupby(["startYear", "genres"])["writers"].nunique().reset_index()
        df4.rename(columns={"writers":"numWriters"}, inplace=True)
    except :
        logging.error("There was a problem while computing metrics")

    merge = partial(pd.merge, on=["startYear", "genres"], how='left')
    df = reduce(merge, [df1, df2, df4, df3])

    # Change data types and round columns
    df["startYear"] = df["startYear"].astype("Int64")
    df["numVotes"] = df["numVotes"].astype("Int64")
    df["runtimeMinutes"] = df["runtimeMinutes"].round(2)
    df["averageRating"] = df["averageRating"].round(2)

    # Save the final results
    logging.info("Saving final results...")
    df.to_csv("/home/airflow/resultados.csv", index = False)
    