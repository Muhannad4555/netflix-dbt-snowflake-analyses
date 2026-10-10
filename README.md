# Netflix-Style Movie Analytics with dbt and Snowflake

A dbt project that turns the raw **MovieLens** movie-ratings dataset into a dimensional model in **Snowflake**, with an incremental fact table, an SCD Type 2 snapshot, data tests and a daily **Airflow** DAG built with **Astronomer Cosmos**.

**MovieLens CSVs → Snowflake `MOVIELENS.RAW` → dbt (staging → dimensions and facts → mart) → Airflow (Cosmos)**

> The data is the public MovieLens dataset (movies, ratings, tags and tag genome). It is not Netflix data; the project models the kind of viewing and rating data a streaming service works with.

## What gets built

| Layer | Models | Materialization | Description |
|---|---|---|---|
| Staging | `src_movies`, `src_ratings`, `src_tags`, `src_links`, `src_genome_tags`, `src_genome_score` | view (`src_ratings` is a table) | One model per raw table. Renames columns to snake_case and converts Unix timestamps with `TO_TIMESTAMP_LTZ` |
| Dimensions | `dim_movies`, `dim_users`, `dim_genome_tags` | table | Cleaned titles and tag names, genres split into an array, one row per user seen in ratings or tags |
| Dimensions | `dim_movies_with_tags` | ephemeral | Movies joined to their genome tags and relevance scores |
| Facts | `fct_ratings` | incremental | One row per user rating. A re-run only adds ratings newer than the latest `rating_timestamp` |
| Facts | `fct_genome_scores` | table | Relevance score of each tag for each movie |
| Mart | `mart_movie_releases` | table | Ratings joined to the release-date seed, flagging whether the release date is known |
| Snapshot | `snap_tags` | snapshot | SCD Type 2 history of user tags (`timestamp` strategy on `tag_timestamp`, hard deletes invalidated) |
| Seed | `seed_movie_release_dates` | seed | Small CSV of movie release dates |
| Analysis | `movie_analysis` | analysis | Top 20 movies by average rating, among movies with more than 100 ratings |

## Tech stack

dbt (dbt-snowflake) · Snowflake · Apache Airflow 3 (Astro Runtime) · Astronomer Cosmos · Docker · `dbt_utils`

## Repository structure

```
├── models/
│   ├── staging/          # 6 src_ models on top of MOVIELENS.RAW
│   ├── dim/              # 4 dimension models
│   ├── fct/              # 2 fact models (fct_ratings is incremental)
│   ├── mart/             # mart_movie_releases
│   ├── schema.yml        # model docs and generic tests
│   └── sources.yml       # the 6 raw source tables
├── snapshots/snap_tags.sql        # SCD Type 2 snapshot
├── seeds/seed_movie_release_dates.csv
├── macros/no_nulls_in_columns.sql # reusable null-check macro
├── tests/relevence_score_test.sql # singular test that uses the macro
├── analyses/movie_analysis.sql
├── packages.yml                   # dbt_utils
└── airflow-netflix/               # Astro project that runs dbt on a schedule
    ├── Dockerfile                 #   Astro Runtime image
    ├── requirements.txt           #   astronomer-cosmos, dbt-snowflake, Snowflake provider
    └── dags/
        ├── netflix_pipeline.py    #   the DAG
        └── dbt/netflix/           #   copy of the dbt project that Cosmos reads
```

## Data quality

- **13 generic tests** in `models/schema.yml`: `not_null` on the keys and measures of every dimension and fact, `unique` on `dim_genome_tags.tag_id`, and a `relationships` test from `fct_ratings.movie_id` to `dim_movies`.
- **1 singular test** (`tests/relevence_score_test.sql`) built on the `no_nulls_in_columns` macro, which loops over every column of `fct_genome_scores` and fails if any value is null.

## Orchestration (Airflow + Cosmos)

The DAG `netflix_dbt_pipeline` runs every day. It uses a Cosmos `DbtTaskGroup`, which reads the dbt project and creates one Airflow task per model, in dependency order, so a failed model stops only the models that depend on it.

The Snowflake credentials are not in the code. Cosmos builds the dbt profile from the Airflow connection `snowflake_default` and targets the `MOVIELENS` database and the `DEV` schema.

## Running it

Prerequisite: the six MovieLens CSVs loaded into Snowflake as `MOVIELENS.RAW.RAW_MOVIES`, `RAW_RATINGS`, `RAW_TAGS`, `RAW_LINKS`, `RAW_GENOME_TAGS` and `RAW_GENOME_SCORES`. The loading scripts are not part of this repository.

**dbt only**

Add a `netflix` profile for Snowflake to `~/.dbt/profiles.yml`, then:

```bash
dbt deps        # install dbt_utils
dbt seed        # load the release-date seed
dbt run         # build staging, dimensions, facts and the mart
dbt snapshot    # build snap_tags
dbt test        # run the 14 tests
```

**With Airflow**

```bash
cd airflow-netflix
astro dev start
```

Open the Airflow UI, create a Snowflake connection named `snowflake_default`, then un-pause `netflix_dbt_pipeline`.

## Design notes

- **Incremental fact table.** `fct_ratings` is the largest table, so it is incremental with `on_schema_change='fail'`: new ratings are appended, and a change in the source columns stops the run instead of silently producing a wrong table.
- **Ephemeral model.** `dim_movies_with_tags` is not stored in Snowflake. dbt inlines it as a CTE into any model that references it.
- **Two copies of the dbt project.** Cosmos needs the dbt project inside the Airflow image, so `airflow-netflix/dags/dbt/netflix/` mirrors the project at the repository root. A change to a model must be made in both places.
