WITH raw_movies as (
    SELECT * FROM MOVIELENS.RAW.RAW_MOVIES

)

SELECT 
movieId as movie_id,
title ,
genres 
FROM raw_movies