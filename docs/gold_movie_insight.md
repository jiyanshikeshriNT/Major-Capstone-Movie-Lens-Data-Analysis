# Gold Transformations

# Gold Movie Insight

## 1. Overview

The `gold.movie_insight` table is a Gold layer asset created to provide movie-level analytical insights.

It combines movie metadata, rating information, and tag information from the Silver layer into a single aggregated table that can be used for reporting and analysis.

---

## 2. Source Tables

The following Silver layer tables are used to create the Gold asset:

- `silver.movie_metadata`
- `silver.ratings`
- `silver.tags`

### silver.movie_metadata

Provides basic movie information required for the Gold table.

Required columns:

- MovieId
- Title
- ReleaseYear

### silver.ratings

Provides user rating information for each movie.

It is used to calculate:

- AverageRating
- HighestRating
- LowestRating
- TotalRatings

### silver.tags

Provides tags assigned to movies by users.

It is used to calculate:

- TotalPeopleTagged
- TotalTags
- DistinctTags
- PopularTag

---

## 3. Transformation Process

### Step 1 - Read Silver Tables

The required Silver tables are loaded from PostgreSQL using PySpark JDBC.

The `silver.ratings` table contains a large amount of data, so JDBC partitioning is used while reading the table to improve processing performance.

---

### Step 2 - Calculate Rating Statistics

Ratings are grouped by `MovieId`.

The following aggregations are calculated:

- `AverageRating` - Average rating received by the movie
- `HighestRating` - Maximum rating received by the movie
- `LowestRating` - Minimum rating received by the movie
- `TotalRatings` - Total number of ratings received by the movie

---

### Step 3 - Calculate Tag Statistics

Tags are grouped by `MovieId`.

The following metrics are calculated:

- `TotalPeopleTagged` - Number of distinct users who tagged the movie
- `TotalTags` - Total number of tags assigned to the movie
- `DistinctTags` - Number of distinct tags associated with the movie

---

### Step 4 - Calculate Popular Tag

The frequency of each tag is calculated for every movie.

A window is created for each `MovieId`, and tags are ordered by their frequency in descending order.

The tag with the highest frequency is selected as `PopularTag`.

If multiple tags have the same frequency, alphabetical order of the tag is used as the tie-breaker.

Null and empty tags are excluded while calculating the popular tag.

---

### Step 5 - Combine Movie Insights

Movie metadata is joined with rating statistics using `MovieId`.

The resulting dataset is then joined with tag statistics and the popular tag information.

Left joins are used so that movies without ratings or tags can still remain in the Gold asset.

For movies without tag information, the following values are filled with `0`:

- TotalPeopleTagged
- TotalTags
- DistinctTags

`PopularTag` is allowed to remain NULL when a movie has no valid tags.

---

## 4. Gold Table

The final transformed data is written to:

`gold.movie_insight`

The table contains the following columns:

| Column | Description |
|---|---|
| MovieId | Unique identifier of the movie |
| ReleaseYear | Release year of the movie |
| MovieTitle | Title of the movie |
| AverageRating | Average rating received by the movie |
| HighestRating | Highest rating received by the movie |
| LowestRating | Lowest rating received by the movie |
| TotalRatings | Total number of ratings |
| TotalPeopleTagged | Number of distinct users who tagged the movie |
| TotalTags | Total number of tags assigned |
| DistinctTags | Number of distinct tags |
| PopularTag | Most frequently used tag for the movie |
| LoadTs | Timestamp when the Gold data was loaded |
| UpdateTs | Timestamp when the Gold data was updated |

---

## 5. Data Flow

```text
silver.movie_metadata
        |
        |---- MovieId, Title, ReleaseYear
        |
        v
   Movie Metadata
        |
        |
silver.ratings
        |
        |---- AVG(Rating)
        |---- MAX(Rating)
        |---- MIN(Rating)
        |---- COUNT(Rating)
        |
        v
   Rating Statistics
        |
        |
silver.tags
        |
        |---- Distinct Users
        |---- Total Tags
        |---- Distinct Tags
        |---- Popular Tag
        |
        v
    Tag Statistics
        |
        v
 Join using MovieId
        |
        v
gold.movie_insight
```

---

## 6. Airflow Orchestration

The Gold transformation is orchestrated using Apache Airflow.

After the required Silver transformations are completed, the Gold pipeline is triggered.

The Gold DAG executes the `transform_movie_insight` task, which runs the PySpark transformation responsible for creating the `gold.movie_insight` table.

The overall orchestration flow is:

```text
Bronze Pipeline
      |
      v
Silver Pipeline
      |
      v
Required Silver Tables
      |
      v
Trigger Gold Pipeline
      |
      v
transform_movie_insight
      |
      v
gold.movie_insight
```

---

## 7. Audit and Logging

After the Gold transformation is executed, audit information is stored in:

`gold.transformation_log`

The log captures:

- Source tables
- Target table
- Status
- Target row count
- Load time
- Error message

For successful executions, the status is stored as `SUCCESS`.

If the transformation fails, the status is stored as `FAILED` along with the corresponding error message.

The load time is automatically recorded in PostgreSQL.

---

## 8. Validation

After writing the Gold asset, the target table row count is retrieved and stored in the transformation log.

The `gold.movie_insight` table contains one row for each movie from the movie metadata source.

The generated Gold asset can be validated by checking:

- Target row count
- Rating aggregations
- Tag aggregations
- Popular tag calculation
- Null handling
- Transformation log status

---

## 9. Final Output

The `gold.movie_insight` asset provides a consolidated movie-level dataset containing movie metadata, rating statistics, and tagging behaviour.

This Gold asset can be directly used for downstream analytics, reporting, and visualization.

---

# Gold User Insight

## 10. Overview

The `gold.user_insight` table is a Gold layer asset created to provide user-level analytical insights.

It combines rating activity, tagging behaviour, movie metadata, and genre information from the Silver layer into a single aggregated table.

The table contains one row per user and can be used for user behaviour analysis, reporting, and downstream visualization.

---

## 11. Source Tables

The following Silver layer tables are used to create the Gold asset:

- `silver.ratings`
- `silver.tags`
- `silver.movie_metadata`

### silver.ratings

Provides the rating activity performed by each user.

It is used to calculate:

- NumberOfMoviesRated
- AverageLifetimeRating
- AverageRatingsPerYear
- LeastRatingGiven
- HighestRatingGiven
- FiveStarMovieCount
- MostRecentlyRatedMovie
- TopGenre
- BestRatedGenre

### silver.tags

Provides the tags assigned by users to movies.

It is used to calculate:

- TotalTagsUsed
- MostUsedTag
- MostRecentlyTaggedMovie

### silver.movie_metadata

Provides movie information required for genre and movie title analysis.

Required columns:

- MovieId
- Title
- Genres

---

## 12. Transformation Process

### Step 1 - Read Required Silver Tables

The required Silver tables are loaded from PostgreSQL using PySpark JDBC.

The `silver.ratings` table contains a large volume of data, so JDBC partitioning is used while reading the table to improve processing performance.

---

### Step 2 - Process Users in Batches

The user-level transformation is performed in `UserId` batches.

A batch size of `25000` users is used.

The UserId range is processed incrementally from the minimum user id to the maximum user id.

This batching approach reduces Spark memory usage while processing the large ratings dataset.

For every batch:

- Ratings are filtered for the current UserId range
- User-level aggregations are calculated
- Related tags and movie metadata are joined
- The transformed result is written to PostgreSQL

The first batch writes the Gold table using overwrite mode, while the remaining batches use append mode.

This ensures that the table is recreated for every complete transformation run without duplicating data from previous executions.

---

### Step 3 - Calculate User Rating Statistics

Ratings are grouped by `UserId`.

The following aggregations are calculated:

- `NumberOfMoviesRated` - Number of distinct movies rated by the user
- `AverageLifetimeRating` - Average of all ratings given by the user
- `LeastRatingGiven` - Lowest rating given by the user
- `HighestRatingGiven` - Highest rating given by the user
- `FiveStarMovieCount` - Number of distinct movies for which the user gave a 5-star rating

---

### Step 4 - Calculate Average Ratings Per Year

The rating Unix timestamp is converted into a timestamp value.

The year is extracted from the rating timestamp.

Ratings are grouped by:

- UserId
- RatingYear

The number of ratings given by each user in each year is calculated.

The average of these yearly rating counts is then calculated for each user and stored as:

`AverageRatingsPerYear`

---

### Step 5 - Calculate Tag Statistics

Tags are grouped by `UserId`.

The total number of tags used by each user is calculated as:

`TotalTagsUsed`

Users with no matching tag information are assigned a value of `0`.

---

### Step 6 - Calculate Most Used Tag

Null and empty tags are excluded.

Tag frequency is calculated for every combination of:

- UserId
- Tag

A window is created for each user.

Tags are ordered by:

1. Tag frequency in descending order
2. Tag value in ascending alphabetical order

The first ranked tag is selected as:

`MostUsedTag`

The alphabetical ordering is used as a deterministic tie-breaker when multiple tags have the same frequency.

---

### Step 7 - Prepare Movie Genres

The `Genres` column from `silver.movie_metadata` contains multiple genres separated using the `|` delimiter.

The genres are split into an array and exploded so that each movie can have one row for each associated genre.

Invalid or unavailable genre values such as:

`(no genres listed)`

are excluded from genre-level calculations.

---

### Step 8 - Calculate Top Genre

Ratings are joined with the exploded movie genre data using `MovieId`.

The number of distinct movies rated by each user in every genre is calculated.

A window is created for each user and genres are ordered by:

1. Number of movies rated in the genre in descending order
2. Genre name in ascending alphabetical order

The highest ranked genre is stored as:

`TopGenre`

This represents the genre in which the user rated the greatest number of movies.

---

### Step 9 - Calculate Best Rated Genre

The average rating given by each user for every genre is calculated.

The number of distinct movies rated within each genre is also calculated.

Genres are ranked using:

1. Genre average rating in descending order
2. Number of movies rated in the genre in descending order
3. Genre name in ascending alphabetical order

The highest ranked genre is stored as:

`BestRatedGenre`

---

### Step 10 - Identify Most Recently Rated Movie

Ratings are joined with movie metadata to retrieve movie titles.

For every user, rating records are ordered by the rating timestamp in descending order.

The most recent record is selected using a window function.

The associated movie title is stored as:

`MostRecentlyRatedMovie`

If timestamps are equal, `MovieId` is used as an additional tie-breaker.

---

### Step 11 - Identify Most Recently Tagged Movie

Tags are joined with movie metadata to retrieve movie titles.

For every user, tag records are ordered by the tag timestamp in descending order.

The latest tagged movie is selected using a window function.

The result is stored as:

`MostRecentlyTaggedMovie`

If timestamps are equal, `MovieId` is used as an additional tie-breaker.

---

### Step 12 - Combine User Insights

The individual user-level datasets are joined using `UserId`.

The following datasets are combined:

- User rating statistics
- Average ratings per year
- Total tags used
- Most used tag
- Top genre
- Best rated genre
- Most recently rated movie
- Most recently tagged movie

Left joins are used so that users remain in the Gold asset even when optional tag information is unavailable.

---

## 13. Gold Table

The final transformed data is written to:

`gold.user_insight`

The table contains the following columns:

| Column | Description |
|---|---|
| UserId | Unique identifier of the user |
| NumberOfMoviesRated | Number of distinct movies rated by the user |
| AverageLifetimeRating | Average rating given by the user |
| AverageRatingsPerYear | Average number of ratings given by the user per active year |
| LeastRatingGiven | Lowest rating given by the user |
| HighestRatingGiven | Highest rating given by the user |
| FiveStarMovieCount | Number of movies rated 5 stars by the user |
| TotalTagsUsed | Total number of tags used by the user |
| MostUsedTag | Most frequently used tag by the user |
| TopGenre | Genre in which the user rated the highest number of movies |
| BestRatedGenre | Genre with the highest average rating given by the user |
| MostRecentlyRatedMovie | Most recently rated movie by the user |
| MostRecentlyTaggedMovie | Most recently tagged movie by the user |
| LoadTs | Timestamp when the Gold data was loaded |
| UpdateTs | Timestamp when the Gold data was updated |

---

## 14. Data Flow

```text
silver.ratings
      |
      |---- User rating statistics
      |---- Ratings per year
      |---- Recent rating activity
      |---- Genre rating activity
      |
      v
 Rating/User Statistics
      |
      |
silver.tags
      |
      |---- Total tags
      |---- Most used tag
      |---- Recent tag activity
      |
      v
   Tag Statistics
      |
      |
silver.movie_metadata
      |
      |---- Movie titles
      |---- Genres
      |
      v
 Movie/Genre Information
      |
      v
Join using UserId
      |
      v
Process in UserId batches
      |
      v
gold.user_insight
```

---

## 15. Airflow Orchestration

The `gold.user_insight` transformation is orchestrated using Apache Airflow.

The Gold DAG executes the `transform_user_insight` task, which runs the PySpark transformation responsible for creating the `gold.user_insight` table.

The overall orchestration flow is:

```text
Bronze Pipeline
      |
      v
Silver Pipeline
      |
      v
Required Silver Tables
      |
      v
Trigger Gold Pipeline
      |
      v
transform_movie_insight
      |
      v
gold.movie_insight
      |
      v
transform_user_insight
      |
      v
gold.user_insight
```

---

## 16. Audit and Logging

After the Gold transformation is executed, audit information is stored in:

`gold.transformation_log`

The log captures:

- Source tables
- Target table
- Status
- Target row count
- Load time
- Error message

For successful executions, the status is stored as `SUCCESS`.

If the transformation fails, the status is stored as `FAILED` along with the corresponding error message.

The load time is automatically recorded in PostgreSQL.

---

## 17. Validation

After writing the Gold asset, the target table row count is retrieved and stored in the transformation log.

The successfully generated `gold.user_insight` table contains:

`200948` rows

representing one user-level insight record for each UserId.

The generated Gold asset can be validated by checking:

- Target row count
- One row per UserId
- Rating aggregation values
- Average ratings per year
- Five-star movie count
- Tag aggregation
- Most used tag
- Top genre
- Best rated genre
- Most recently rated movie
- Most recently tagged movie
- Audit timestamps
- Transformation log status

---

## 18. Performance Handling

The user insight transformation performs multiple joins, aggregations, and window operations on the ratings dataset.

Processing the complete user population together caused high Spark memory consumption.

To handle this efficiently, users are processed in batches of:

`25000`

The batches are processed sequentially.

Example:

```text
UserId 1      - 25000
UserId 25001  - 50000
UserId 50001  - 75000
UserId 75001  - 100000
UserId 100001 - 125000
UserId 125001 - 150000
UserId 150001 - 175000
UserId 175001 - 200000
UserId 200001 - 200948
```

This approach reduces memory pressure and allows the complete user-level Gold transformation to execute successfully.

---

## 19. Final Output

The `gold.user_insight` asset provides a consolidated user-level analytical dataset containing rating behaviour, tagging activity, genre preferences, and recent user activity.

Together, the following Gold assets provide the analytical layer created so far:

- `gold.movie_insight`
- `gold.user_insight`

These Gold assets can be used for downstream analytics, business reporting, and visualization.