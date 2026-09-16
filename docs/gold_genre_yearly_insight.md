# Gold Genre and Yearly Insights

# Part 1 - Gold Genre Insight

## 1. Overview

The `gold.genre_insight` table is a Gold layer asset created to provide genre-level analytical insights from the MovieLens dataset.

It combines movie metadata, ratings, and tag information from the Silver layer and produces one aggregated record for each movie genre.

The table can be used for genre-level reporting, analysis, and downstream visualization.

---

## 2. Source Tables

The following Silver layer tables are used to create the Genre Insight Gold asset:

- `silver.movie_metadata`
- `silver.ratings`
- `silver.tags`

### silver.movie_metadata

Provides movie-level metadata required for genre analysis.

The important columns used are:

- `MovieId`
- `Title`
- `ReleaseYear`
- `Genres`

Since a movie can belong to multiple genres, the `Genres` column is split using the `|` delimiter and exploded so that each movie can be associated with each of its individual genres.

Movies containing `(no genres listed)` are excluded from genre-level analysis.

### silver.ratings

Provides rating information given by users to movies.

It is used to calculate:

- Total ratings for each genre
- Average rating for each genre
- Rating distribution
- Most rated movie
- Least rated movie
- Highest average rated movie
- Top 10 popular movies
- Top 10 highly rated movies
- Worst rated movie

Because the ratings table contains a large amount of data, the records are processed in MovieId batches to reduce Spark memory usage.

### silver.tags

Provides user-generated tags assigned to movies.

It is used to calculate the most frequently used tag for each genre.

Null and empty tags are excluded while calculating the popular tag.

---

## 3. Transformation Process

### Step 1 - Read Movie Metadata

The `silver.movie_metadata` table is loaded from PostgreSQL using PySpark JDBC.

The required movie information is selected from the table.

---

### Step 2 - Prepare Movie-to-Genre Mapping

The `Genres` column may contain multiple genre values separated by `|`.

The genre string is split into an array and exploded so that each movie can have multiple genre records.

Example:

```text
MovieId | Genres
--------|-------------------------
1       | Adventure|Animation
```

is transformed into:

```text
MovieId | Genre
--------|----------
1       | Adventure
1       | Animation
```

Invalid or empty genre values are excluded.

This creates the movie-to-genre mapping required for further aggregations.

---

### Step 3 - Calculate Movie Count per Genre

The movie-to-genre mapping is grouped by `Genre`.

The number of distinct movies belonging to each genre is calculated and stored as:

`MovieCount`

This represents the total number of movies available under each genre.

---

### Step 4 - Process Ratings in Batches

The Silver ratings dataset contains millions of records and requires memory-intensive grouping and aggregation operations.

To avoid Java heap space and OutOfMemory errors, ratings are processed using MovieId ranges instead of processing the complete dataset in a single operation.

The configured batch size is:

```text
25,000 MovieIds per batch
```

Example processing flow:

```text
MovieId 1 - 25000
MovieId 25001 - 50000
MovieId 50001 - 75000
...
Final MovieId batch
```

Each ratings batch is joined with the movie-to-genre mapping and the required intermediate statistics are calculated.

This reduces the amount of data being processed simultaneously by Spark.

---

## 4. Genre-Level Rating Statistics

After combining ratings with the movie genre mapping, rating information is aggregated for each genre.

The following metrics are produced:

### TotalRatings

Total number of ratings received by movies belonging to the genre.

### AverageRating

Average rating across the rating records associated with movies in the genre.

---

## 5. Rating Distribution

The Gold asset provides the rating distribution for every genre.

Separate count columns are created for the MovieLens rating values:

- `Rated0Count`
- `Rated05Count`
- `Rated1Count`
- `Rated15Count`
- `Rated2Count`
- `Rated25Count`
- `Rated3Count`
- `Rated35Count`
- `Rated4Count`
- `Rated45Count`
- `Rated5Count`

These columns make it possible to analyze how ratings are distributed across individual genres.

---

## 6. Popular Tag per Genre

Tag records are associated with movies and then mapped to their corresponding genres.

For each genre, the frequency of every valid tag is calculated.

A Window operation ranks the tags based on their frequency.

The most frequently occurring tag is selected as:

`PopularTag`

Null and empty tags are ignored during this calculation.

When multiple tags have the same frequency, deterministic ordering is used so that one tag is selected consistently.

---

## 7. Movie-Level Genre Statistics

Movie-level rating statistics are calculated before determining the important movies for each genre.

These statistics are used to identify movies based on:

- Number of ratings
- Average rating
- Release information

The following analytical columns are generated.

### MostRatedMovie

The movie within the genre having the highest number of ratings.

### LeastRatedMovie

The movie within the genre having the lowest number of ratings.

### HighestAverageRatedMovie

The movie having the highest average rating within the genre while satisfying the minimum rating-count condition required by the project.

For this metric, only movies having at least:

```text
100 ratings
```

are considered.

This prevents movies with very few ratings from being treated as the highest-rated movie only because of a small sample size.

---

## 8. Top 10 Popular Movies

Movies are ranked within each genre according to their total number of ratings.

The top 10 movies are collected into an array and stored in:

`Top10PopularMovies`

This provides a compact list of the most frequently rated movies for each genre.

---

## 9. Top 10 Highly Rated Movies

Movies are ranked according to their average rating while applying the required rating-count conditions.

The top ranked movies are collected into an array and stored in:

`Top10HighlyRatedMovies`

This allows downstream analysis to directly access the highly rated movies for each genre.

---

## 10. Worst Rated Movie

The movie having the lowest average rating in the genre is selected as:

`WorstRatedMovie`

If multiple movies satisfy the same lowest-rating condition, the configured ordering is used to select a consistent record.

The project requirement specifies that when multiple candidates exist, the most recent movie is preferred.

---

## 11. Gold Audit Columns

Two audit columns are added to the Gold dataset:

- `LoadTs`
- `UpdateTs`

Both columns store timestamps associated with the Gold transformation execution.

---

## 12. Final Gold Table

The resulting dataset is written to:

`gold.genre_insight`

The Gold table contains the following columns:

| Column | Description |
|---|---|
| Genre | Movie genre |
| MovieCount | Number of movies belonging to the genre |
| TotalRatings | Total ratings associated with movies in the genre |
| AverageRating | Average rating of movies in the genre |
| PopularTag | Most frequently used valid tag for the genre |
| Rated0Count | Count for rating value 0 |
| Rated05Count | Count for rating value 0.5 |
| Rated1Count | Count for rating value 1 |
| Rated15Count | Count for rating value 1.5 |
| Rated2Count | Count for rating value 2 |
| Rated25Count | Count for rating value 2.5 |
| Rated3Count | Count for rating value 3 |
| Rated35Count | Count for rating value 3.5 |
| Rated4Count | Count for rating value 4 |
| Rated45Count | Count for rating value 4.5 |
| Rated5Count | Count for rating value 5 |
| MostRatedMovie | Movie receiving the highest number of ratings in the genre |
| LeastRatedMovie | Movie receiving the lowest number of ratings in the genre |
| HighestAverageRatedMovie | Highest average rated movie having the required minimum number of ratings |
| Top10PopularMovies | Array containing the top 10 popular movies |
| Top10HighlyRatedMovies | Array containing the top 10 highly rated movies |
| WorstRatedMovie | Lowest rated movie for the genre |
| LoadTs | Gold data load timestamp |
| UpdateTs | Gold data update timestamp |

---

## 13. Data Flow

```text
silver.movie_metadata
        |
        |---- MovieId
        |---- Title
        |---- ReleaseYear
        |---- Genres
        |
        v
Split and Explode Genres
        |
        v
Movie - Genre Mapping
        |
        |-------------------------------|
        |                               |
        v                               v
Genre Movie Count                silver.ratings
                                        |
                                        |  Processed in
                                        |  MovieId batches
                                        v
                              Genre Rating Statistics
                                        |
                                        |---- TotalRatings
                                        |---- AverageRating
                                        |---- Rating Distribution
                                        |
                                        v
                              Movie-Level Statistics
                                        |
                                        |---- Most Rated Movie
                                        |---- Least Rated Movie
                                        |---- Highest Avg Rated Movie
                                        |---- Top 10 Popular Movies
                                        |---- Top 10 Highly Rated Movies
                                        |---- Worst Rated Movie
                                        |
silver.tags                             |
        |                               |
        v                               |
Genre Tag Frequency                    |
        |                               |
        v                               |
Popular Tag                            |
        |                               |
        |-------------------------------|
                        |
                        v
                Combine by Genre
                        |
                        v
               gold.genre_insight
```

---

## 14. Airflow Orchestration

The Genre Insight transformation is orchestrated using Apache Airflow.

The Gold DAG contains separate tasks for the Gold assets.

The `transform_genre_insight` task runs:

```text
python -m src.pyspark.transform_gold transform_genre_insight
```

Since the Gold transformations are resource-intensive, the tasks are executed sequentially instead of running all large Spark transformations in parallel.

The Gold task flow is:

```text
transform_movie_insight
        |
        v
transform_user_insight
        |
        v
transform_genre_insight
```

This prevents multiple Spark jobs from competing for the available memory at the same time.

The complete pipeline flow is:

```text
Bronze Ingestion
        |
        v
Silver Transformation
        |
        v
Gold Transformation
        |
        v
transform_movie_insight
        |
        v
transform_user_insight
        |
        v
transform_genre_insight
        |
        v
gold.genre_insight
```

---

## 15. Audit and Logging

Execution information for the Genre Insight transformation is stored in:

`gold.transformation_log`

The log records:

- Source tables
- Target table
- Transformation status
- Target row count
- Load time
- Error message

For successful execution:

```text
target_table = gold.genre_insight
status       = SUCCESS
```

If an exception occurs, the transformation writes:

```text
status = FAILED
```

along with the corresponding error message.

This makes Gold pipeline executions traceable and helps with troubleshooting.

---

## 16. Validation

The Genre Insight transformation is validated after the table is written to PostgreSQL.

The following checks are used:

- Successful completion of the PySpark transformation
- Successful creation of `gold.genre_insight`
- Verification of the final schema
- Verification of the target row count
- Verification of genre-level rating aggregations
- Verification of rating distribution columns
- Verification of the popular tag calculation
- Verification of movie ranking metrics
- Verification of audit columns
- Verification of the Gold transformation log

For the current MovieLens dataset, the transformation produces:

```text
gold.genre_insight row count: 19
```

The successful execution is also recorded in:

```text
gold.transformation_log
```

with status:

```text
SUCCESS
```

---

## 17. Performance and Memory Handling

The ratings dataset contains millions of records, which caused memory pressure when large aggregations were performed in a single Spark operation.

To make the transformation stable on the available environment, the processing was changed to MovieId-based batching.

A batch size of:

```text
25000
```

is used.

This approach reduces the amount of rating data processed at one time while still allowing the final genre-level statistics to be calculated.

The Gold Airflow tasks are also executed sequentially so that multiple memory-intensive Spark jobs are not executed simultaneously.

---

## 18. Final Output

The `gold.genre_insight` table provides a consolidated analytical view of movie genres.

It contains:

- Number of movies in each genre
- Total and average ratings
- Rating distribution
- Most popular tag
- Most and least rated movies
- Highest average rated movie
- Top 10 popular movies
- Top 10 highly rated movies
- Worst rated movie
- Audit timestamps

The generated Gold asset can be used directly for reporting, analytics, and visualization.

---

# Part 2 - Gold Yearly Insight

## 1. Overview

The `gold.yearly_insight` table is a Gold layer asset created to provide year-level analytical insights from the MovieLens dataset.

It combines movie metadata, ratings, genres, and tag information from the Silver layer and produces one consolidated analytical record for each movie release year.

The table can be used for downstream reporting, business analysis, and visualization.

---

## 2. Source Tables

The following Silver layer tables are used to create the yearly insight asset:

- `silver.movie_metadata`
- `silver.ratings`
- `silver.tags`

### silver.movie_metadata

Provides movie information required for yearly analysis.

Required columns:

- MovieId
- Title
- ReleaseYear
- Genres

### silver.ratings

Provides user rating information.

It is used to calculate:

- Total number of ratings
- Most popular movie
- Highly rated movie
- Highest average rating
- Worst rated movie
- Lowest rating
- Genre-level rating statistics

### silver.tags

Provides user-generated movie tags.

It is used to calculate:

- Total number of distinct tags
- Most popular tag

---

## 3. Transformation Process

### Step 1 - Read Movie Metadata

The `silver.movie_metadata` table is loaded from PostgreSQL using PySpark JDBC.

Movie metadata is used to identify:

- MovieId
- Movie title
- Release year
- Genres

Only movies having a valid release year are considered for year-level analysis.

---

### Step 2 - Prepare Movie and Year Mapping

A mapping between each movie and its release year is created from `silver.movie_metadata`.

```text
MovieId
   |
   v
ReleaseYear
```

This mapping is later used to associate ratings and tags with the corresponding movie release year.

---

### Step 3 - Process Large Rating and Tag Data in Batches

The ratings and tags datasets contain a large number of records.

To reduce memory usage and avoid Spark `OutOfMemoryError`, the data is processed in MovieId batches.

The processing uses:

```text
Batch Size = 25,000 MovieIds
```

For each MovieId batch:

1. Required rating records are read from PostgreSQL.
2. Required tag records are read from PostgreSQL.
3. Batch-level aggregations are calculated.
4. Aggregated results are combined for the final yearly calculations.

This approach avoids loading and processing all rating and tag records in memory at the same time.

---

## 4. Year-Level Movie Statistics

### Number of Movies Released

Movies are grouped by `ReleaseYear`.

The number of distinct movies released in each year is calculated.

---

### Most Popular Movie

Movie popularity is determined using the number of ratings received by each movie.

For every year, the movie having the highest number of ratings is selected as:

`MostPopularMovie`

---

### Total Number of Ratings

All ratings received by movies released in a particular year are counted.

The result is stored as:

`TotalRatings`

---

### Highly Rated Movie

Movie-level average ratings are calculated.

The highly rated movie for each year is selected based on the calculated movie rating statistics.

The result is stored as:

`HighlyRatedMovie`

---

### Highest Average Rating

The maximum movie-level average rating achieved by any movie released in the year is calculated.

The result is stored as:

`HighestAverageRating`

---

### Worst Rated Movie

Movie rating statistics are evaluated to identify the lowest-performing movie for every release year.

The result is stored as:

`WorstRatedMovie`

---

### Lowest Rating

The minimum individual rating received by any movie released in a particular year is calculated.

The result is stored as:

`LowestRating`

---

## 5. Genre-Level Analysis

Movies can belong to multiple genres.

The `Genres` column contains genre values separated by `|`.

Example:

```text
MovieId | Genres
------------------------
1       | Action|Comedy
```

The genre values are split and exploded into separate rows:

```text
MovieId | Genre
----------------
1       | Action
1       | Comedy
```

This allows rating statistics to be calculated independently for every genre.

---

### Top Rated Genre

Ratings are associated with the genres of their corresponding movies.

Genre-level rating statistics are calculated for each release year.

The highest-performing genre for every year is selected as:

`TopRatedGenre`

---

### Least Rated Genre

The lowest-performing genre for every year is selected using genre-level rating statistics.

The result is stored as:

`LeastRatedGenre`

---

## 6. Tag Analysis

Tags are associated with the corresponding movie release year using `MovieId`.

### Total Distinct Tags

The number of unique tags associated with movies released in each year is calculated.

The result is stored as:

`TotalDistinctTags`

---

### Most Popular Tag

The frequency of every valid tag is calculated for each release year.

The most frequently occurring tag is selected as:

`MostPopularTag`

Null and empty tag values are excluded while calculating the popular tag.

---

## 7. Final Gold Table

The transformed output is written to:

`gold.yearly_insight`

The table contains the following columns:

| Column | Description |
|---|---|
| Year | Movie release year |
| NumberOfMoviesReleased | Number of movies released in the year |
| MostPopularMovie | Movie having the highest number of ratings |
| TotalRatings | Total ratings received by movies released in the year |
| HighlyRatedMovie | Highly rated movie for the year |
| HighestAverageRating | Highest movie-level average rating in the year |
| WorstRatedMovie | Lowest-performing movie in the year |
| LowestRating | Lowest rating received by any movie released in the year |
| TopRatedGenre | Best performing genre in the year |
| LeastRatedGenre | Lowest performing genre in the year |
| TotalDistinctTags | Number of distinct tags associated with movies released in the year |
| MostPopularTag | Most frequently used tag in the year |
| LoadTs | Timestamp when the Gold data was loaded |
| UpdateTs | Timestamp when the Gold data was updated |

---

## 8. Data Flow

```text
silver.movie_metadata
        |
        |---- MovieId
        |---- Title
        |---- ReleaseYear
        |---- Genres
        |
        v
Movie / Year / Genre Mapping
        |
        +----------------------------+
        |                            |
        v                            v
silver.ratings                  silver.tags
        |                            |
        |---- Rating Count           |---- Distinct Tags
        |---- Average Rating         |---- Tag Frequency
        |---- Highest Rating         |
        |---- Lowest Rating          |
        |                            |
        v                            v
Rating Statistics              Tag Statistics
        |                            |
        +-------------+--------------+
                      |
                      v
              Yearly Aggregation
                      |
                      v
             gold.yearly_insight
```

---

## 9. Airflow Orchestration

The yearly insight transformation is orchestrated using Apache Airflow.

The Gold DAG executes the PySpark task:

`transform_yearly_insight`

The Gold transformations are executed sequentially.

The Gold pipeline flow is:

```text
transform_movie_insight
          |
          v
transform_user_insight
          |
          v
transform_genre_insight
          |
          v
transform_yearly_insight
          |
          v
gold.yearly_insight
```

Sequential execution is used because the Gold transformations contain large aggregations, joins, sorting operations, and window operations.

Running all Gold transformations in parallel can increase memory consumption and may cause Spark memory-related failures.

---

## 10. Audit and Logging

Transformation execution details are stored in:

`gold.transformation_log`

The log captures:

- Source tables
- Target table
- Status
- Target row count
- Load time
- Error message

For a successful execution:

```text
Status = SUCCESS
Target Table = gold.yearly_insight
```

If the transformation fails:

```text
Status = FAILED
```

The corresponding error message is stored in the transformation log for debugging and audit purposes.

---

## 11. Validation

After writing the Gold table to PostgreSQL, the target row count is retrieved using the common row-count utility.

The generated Gold asset can be validated by checking:

- Valid year values
- Number of movies released
- Total rating counts
- Most popular movie
- Highly rated movie
- Highest average rating
- Worst rated movie
- Lowest rating
- Top rated genre
- Least rated genre
- Total distinct tags
- Most popular tag
- Target row count
- Gold transformation log status

The resulting table contains one analytical row for each valid movie release year present in the source data.

---

## 12. Final Output

The `gold.yearly_insight` table provides a consolidated year-level analytical dataset.

It summarizes:

- Movie releases
- Movie popularity
- Rating behaviour
- Genre performance
- Tagging activity

The final Gold asset can be directly used for downstream reporting, analytical queries, and Tableau visualization.