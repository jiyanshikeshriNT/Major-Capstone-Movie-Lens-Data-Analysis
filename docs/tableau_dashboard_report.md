# MovieLens Tableau Dashboard Report

## 1. Overview

The final visualization layer of the MovieLens Data Engineering project was implemented using **Tableau**.

The objective of the visualization layer is to convert the processed MovieLens data into meaningful business insights related to:

- Movies
- Ratings
- Genres
- Release years
- Users
- Tags
- User activity
- Movie engagement

The Tableau implementation contains two major analytical views:

1. **Movie & Ratings Insights**
2. **User, Tags & Engagement Analysis**

The primary analytical datasets are available in the Gold layer. However, during dashboard development, a gap analysis identified some dashboard requirements that required row-level data, rating timestamps, tag-level details, or multiple dimensions at the same grain.

To support these requirements without changing the existing Gold analytical tables, additional PostgreSQL reporting views were created in the `gold` schema using cleaned Silver-layer data.

Therefore, the final Tableau solution uses:

```text
Silver Layer
     │
     ├── Gold Analytical Tables
     │
     └── PostgreSQL Reporting Views
                    │
                    ▼
                 Tableau
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
      Dashboard 1         Dashboard 2
   Movie & Ratings     Users, Tags &
      Insights           Engagement
```

---

# 2. Gold Analytical Tables Used

The following Gold analytical datasets were created by the PySpark transformation pipeline:

| Gold Asset | Purpose |
|---|---|
| `gold.movie_insight` | Movie-level rating and tagging statistics |
| `gold.user_insight` | User-level rating and engagement statistics |
| `gold.genre_insight` | Genre-level movie and rating analytics |
| `gold.yearly_insight` | Release-year-level movie analytics |

These tables support most of the aggregated KPIs and visualizations required by the Tableau dashboards.

---

# 3. Additional PostgreSQL Reporting Views

Some Tableau requirements could not be directly supported by the aggregated Gold tables because the required row-level dimensions or timestamps were no longer available at the Gold grain.

Seven PostgreSQL views were therefore created to expose the required data for Tableau.

The views are:

1. `gold.rating_distribution_view`
2. `gold.rating_time_view`
3. `gold.tag_frequency_view`
4. `gold.view1_dashboard_base`
5. `gold.view2_tag_detail`
6. `gold.view2_matrix_detail`
7. `gold.view2_movie_engagement`

---

## 3.1 Rating Distribution View

### Purpose

This view provides the number of ratings submitted for each rating value.

It supports the **Ratings Distribution** visualization in Tableau.

### SQL

```sql
CREATE OR REPLACE VIEW gold.rating_distribution_view AS
SELECT
    "Rating" AS rating,
    COUNT(*) AS rating_count
FROM silver.ratings
WHERE "Rating" IS NOT NULL
GROUP BY "Rating";
```

### Output Grain

One row per rating value.

Example:

```text
Rating | Rating Count
-------|-------------
0.5    | ...
1.0    | ...
1.5    | ...
...
5.0    | ...
```

This avoids processing the complete ratings dataset directly in Tableau for the rating distribution visualization.

---

## 3.2 Rating Time View

### Purpose

The Gold analytical tables do not retain the individual rating event timestamp at the grain required for monthly rating activity analysis.

This view aggregates rating activity by month and supports the **Ratings Submitted Over Time** visualization.

### SQL

```sql
CREATE OR REPLACE VIEW gold.rating_time_view AS
SELECT
    DATE_TRUNC('month', "Timestamp")::date AS "RatingMonth",
    EXTRACT(YEAR FROM "Timestamp")::integer AS "RatingYear",
    COUNT(*) AS "TotalRatings"
FROM silver.ratings
WHERE "Timestamp" IS NOT NULL
GROUP BY
    DATE_TRUNC('month', "Timestamp")::date,
    EXTRACT(YEAR FROM "Timestamp")::integer;
```

### Output

The view contains:

- Rating month
- Rating year
- Total ratings submitted

This allows Tableau to analyze rating activity over time without loading all rating-level records for this chart.

---

## 3.3 Tag Frequency View

### Purpose

The dashboard requires identification and analysis of the most frequently used tags.

The existing Gold tables contain popular tags at specific analytical grains, but a global tag-frequency dataset was required for the **Most Used Tag** KPI and related tag analysis.

### SQL

```sql
CREATE OR REPLACE VIEW gold.tag_frequency_view AS
SELECT
    TRIM("Tag") AS "Tag",
    COUNT(*) AS "TagCount"
FROM silver.tags
WHERE "Tag" IS NOT NULL
  AND TRIM("Tag") <> ''
GROUP BY TRIM("Tag");
```

### Output Grain

One row per unique tag.

The view can be sorted by `TagCount` in descending order to identify the most frequently used tag in the MovieLens dataset.

---

## 3.4 View 1 Dashboard Base

### Purpose

The Gold analytical tables have different grains such as movie, genre, user, and year.

For some Tableau interactions and filters, row-level rating information together with movie dimensions was required.

`view1_dashboard_base` provides these fields from the cleaned Silver master dataset.

### SQL

```sql
CREATE OR REPLACE VIEW gold.view1_dashboard_base AS
SELECT
    "UserId" AS user_id,
    "MovieId" AS movie_id,
    "Title" AS movie_title,
    "ReleaseYear" AS release_year,
    "Genres" AS genres,
    "Rating" AS rating,
    "RatingTstmp" AS rating_timestamp,
    "Tags" AS tags
FROM silver.user_ratings_master;
```

### Available Dimensions and Measures

The view provides:

- User ID
- Movie ID
- Movie title
- Release year
- Genres
- Rating
- Rating timestamp
- Tags

It acts as a detailed reporting dataset for requirements where multiple dimensions are required together.

---

## 3.5 View 2 Tag Detail

### Purpose

The User, Tags & Engagement dashboard requires detailed tag information along with movie information.

This view combines individual tag events with movie metadata.

### SQL

```sql
CREATE OR REPLACE VIEW gold.view2_tag_detail AS
SELECT
    t."UserId" AS user_id,
    t."MovieId" AS movie_id,
    mm."Title" AS movie_title,
    mm."ReleaseYear" AS release_year,
    mm."Genres" AS genres,
    TRIM(t."Tag") AS tag,
    t."Timestamp" AS tag_timestamp
FROM silver.tags t
LEFT JOIN silver.movie_metadata mm
    ON t."MovieId" = mm."MovieId"
WHERE t."Tag" IS NOT NULL
  AND TRIM(t."Tag") <> '';
```

### Purpose in Tableau

It provides detailed information such as:

```text
User → Movie → Tag
```

along with:

- Movie title
- Release year
- Genre
- Tag timestamp

This dataset supports detailed tag analysis and filtering.

---

## 3.6 View 2 Matrix Detail

### Purpose

The Tableau requirement includes a matrix showing:

```text
Movie → User → Tags → Rating
```

The aggregated Gold tables cannot provide this row-level relationship.

Therefore, the cleaned `silver.user_ratings_master` dataset is exposed through this reporting view.

### SQL

```sql
CREATE OR REPLACE VIEW gold.view2_matrix_detail AS
SELECT
    "UserId" AS user_id,
    "MovieId" AS movie_id,
    "Title" AS movie_title,
    "ReleaseYear" AS release_year,
    "Genres" AS genres,
    "Tags" AS tags,
    "Rating" AS rating
FROM silver.user_ratings_master
WHERE "Tags" IS NOT NULL
  AND TRIM("Tags") <> '';
```

### Purpose in Tableau

This view allows Tableau to display the relationship between:

- Movie
- User
- Tags
- Rating

at a detailed user-movie level.

---

## 3.7 View 2 Movie Engagement

### Purpose

The second dashboard requires a **Tags Count vs Ratings Count per Movie** comparison.

To make rating activity and tagging activity available in the same dataset, tag counts are first calculated for each user-movie combination and then joined with `silver.user_ratings_master`.

### SQL

```sql
CREATE OR REPLACE VIEW gold.view2_movie_engagement AS
WITH tag_counts AS (
    SELECT
        "UserId",
        "MovieId",
        COUNT(*) AS tag_count
    FROM silver.tags
    WHERE "Tag" IS NOT NULL
      AND TRIM("Tag") <> ''
    GROUP BY
        "UserId",
        "MovieId"
)
SELECT
    urm."UserId" AS user_id,
    urm."MovieId" AS movie_id,
    urm."Title" AS movie_title,
    urm."ReleaseYear" AS release_year,
    urm."Genres" AS genres,
    urm."Tags" AS tags,
    urm."Rating" AS rating,
    1 AS rating_count,
    COALESCE(tc.tag_count, 0) AS tag_count
FROM silver.user_ratings_master urm
LEFT JOIN tag_counts tc
    ON urm."UserId" = tc."UserId"
   AND urm."MovieId" = tc."MovieId";
```

### Logic

Each record in `user_ratings_master` represents a user-movie rating relationship.

Therefore:

```text
rating_count = 1
```

is assigned to each row.

Tag activity is calculated separately at:

```text
UserId + MovieId
```

grain.

If no tag exists for a particular user-movie combination:

```sql
COALESCE(tc.tag_count, 0)
```

converts the missing tag count to zero.

Tableau can then aggregate:

```text
SUM(rating_count)
SUM(tag_count)
```

to compare rating activity and tagging activity for movies.

This supports the **Tags Count vs Ratings Count per Movie** scatter plot.

---

# 4. Tableau Dashboard 1 — Movie & Ratings Insights

The first dashboard focuses on overall movie and rating behavior.

## KPIs

The dashboard includes analytical KPIs such as:

- Total Movies
- Distinct Genres
- Total Ratings
- Average Rating Overall
- Top Rated Movie
- Most Rated Genre

The appropriate Gold analytical tables are used for aggregated KPIs to preserve their validated analytical grain.

---

## Visualizations

### Average Rating by Genre

Displays the average rating associated with each genre.

Primary source:

```text
gold.genre_insight
```

---

### Movies by Genre

Shows the number of movies available in each genre.

Primary source:

```text
gold.genre_insight
```

---

### Movie Releases Over Time

Shows how the number of movie releases changes across years.

Primary source:

```text
gold.yearly_insight
```

---

### Ratings Distribution

Shows the frequency of individual rating values.

Source:

```text
gold.rating_distribution_view
```

---

### Top 10 Movies by Number of Ratings

Ranks movies based on the total number of ratings received.

Primary source:

```text
gold.movie_insight
```

---

### Ratings Count vs Average Rating

Compares movie popularity with average audience rating.

Primary source:

```text
gold.movie_insight
```

This helps distinguish movies with high average ratings from movies that also have a large number of ratings.

---

### Ratings Submitted Over Time

Shows monthly rating activity.

Source:

```text
gold.rating_time_view
```

The view uses the rating timestamp retained in `silver.ratings`.

---

## Filters

Dashboard analysis uses relevant dimensions such as:

- Genre
- Release Year
- Rating

Because Gold assets have different analytical grains, filters are applied where the corresponding dimensions are semantically available rather than changing validated KPI definitions only to force a common grain.

---

# 5. Tableau Dashboard 2 — User, Tags & Engagement Analysis

The second dashboard focuses on user activity, tagging behavior, and movie engagement.

## KPIs

The dashboard includes:

- Total Users
- Average Ratings per User
- Most Active User
- Total Tags
- Most Tagged Movie
- Most Used Tag

The aggregated Gold assets provide the primary user/movie metrics, while `gold.tag_frequency_view` supports global tag-frequency analysis.

---

## Visualizations

### Top 10 Users by Number of Ratings

Displays the most active users based on rating activity.

Primary source:

```text
gold.user_insight
```

---

### User Behavior Analysis

Users can be analyzed according to their level of rating activity.

The user-level metrics available in:

```text
gold.user_insight
```

support this analysis.

---

### Ratings Activity by Day of Week vs Time of Day

The dashboard requirement includes analysis of rating activity using the rating timestamp.

The Silver layer retains the cleaned rating timestamp, allowing time-based reporting when detailed event-level analysis is required.

---

### Top 10 Movies by Number of Tags

Ranks movies based on tagging activity.

Movie-level tag metrics from:

```text
gold.movie_insight
```

support this analysis.

---

### Movie → User → Tags → Rating Matrix

Source:

```text
gold.view2_matrix_detail
```

This visualization provides detailed relationships between:

```text
Movie
  ↓
User
  ↓
Tags
  ↓
Rating
```

The reporting view is required because this detailed relationship is not retained in the aggregated Gold analytical tables.

---

### Tags Count vs Ratings Count per Movie

Source:

```text
gold.view2_movie_engagement
```

This visualization compares:

```text
Rating Activity
       vs
Tagging Activity
```

and helps analyze whether highly rated or frequently rated movies also generate greater tagging engagement.

---

## Tag Detail Analysis

Source:

```text
gold.view2_tag_detail
```

This view provides individual tag records along with movie title, genre, release year, and tag timestamp.

It supports detailed tag-level exploration and filtering.

---

# 6. Why Additional Views Were Required

The Gold layer was intentionally designed using aggregated analytical grains.

For example:

```text
movie_insight   → one row per movie
user_insight    → one row per user
genre_insight   → one row per genre
yearly_insight  → one row per release year
```

This structure is efficient for predefined analytical KPIs.

However, some Tableau requirements require data at a lower grain.

Examples include:

```text
Rating distribution
Monthly rating activity
Global tag frequency
Movie → User → Tags → Rating
User-movie tag details
Tags Count vs Ratings Count
```

These requirements depend on information such as:

- User ID
- Movie ID
- Individual rating
- Rating timestamp
- Individual tag
- Tag timestamp
- Genre
- Release year

Instead of modifying the validated Gold analytical tables, lightweight PostgreSQL reporting views were created over the cleaned Silver datasets.

This keeps the architecture logically separated:

```text
Silver
  │
  ├── Clean detailed datasets
  │
  ├──────────────► Reporting Views ──────┐
  │                                      │
  ▼                                      ▼
Gold Analytical Tables ─────────────► Tableau
```

---

# 7. Tableau Data Strategy

The Tableau implementation uses a combination of:

### Aggregated Gold Tables

Used where precomputed analytical metrics already exist.

Advantages:

- Less computation inside Tableau
- Smaller analytical datasets
- Reusable business metrics
- Faster aggregated analysis

### PostgreSQL Reporting Views

Used where the dashboard requires detailed dimensions or event-level information that is intentionally not retained in aggregated Gold tables.

Advantages:

- Avoids duplicating large physical datasets
- Preserves detailed Silver information
- Provides Tableau-specific reporting datasets
- Keeps Gold transformations focused on reusable analytical assets

---

# 8. Data Quality Considerations

Tableau does not directly consume the raw CSV files.

The visualization layer is built only after data passes through the processing pipeline:

```text
Raw MovieLens CSV
        ↓
Bronze
        ↓
Silver
        ↓
Gold / Reporting Views
        ↓
Tableau
```

The Silver layer performs data standardization and deduplication before the data is used for analytics.

Rejected duplicate records from the base Silver transformations are handled through the quarantine mechanism.

This ensures that Tableau uses processed analytical data instead of raw source data.

---

# 9. Timestamp Handling

MovieLens rating and tag timestamps originate as Unix epoch values.

During Silver transformation, timestamps are converted into usable timestamp values.

The Spark session timezone is explicitly standardized to UTC:

```text
spark.sql.session.timeZone = UTC
```

This ensures consistent timestamp interpretation across pipeline executions.

The cleaned timestamps are then used by reporting views such as:

```text
gold.rating_time_view
gold.view2_tag_detail
```

for time-based Tableau analysis.

---

# 10. Dashboard Performance

The MovieLens dataset contains more than 32 million ratings, so directly loading every raw record into every Tableau worksheet would be inefficient.

The project therefore uses multiple optimization strategies:

- Pre-aggregated Gold analytical tables
- PostgreSQL aggregation views
- Tableau-specific reporting views
- Only the required columns exposed to individual views
- Aggregation performed before visualization where possible

The Tableau workbook is packaged as a `.twbx` workbook using extracted data rather than depending on a live database connection for final delivery.

---

# 11. Final Tableau Architecture

```text
MovieLens CSV Files
        │
        ▼
┌─────────────────────┐
│       BRONZE        │
│   Raw Ingestion     │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│       SILVER        │
│ Clean + Standardize │
│ Deduplicate         │
│ Quarantine          │
└──────────┬──────────┘
           │
           ├─────────────────────────────┐
           │                             │
           ▼                             ▼
┌─────────────────────┐       ┌─────────────────────┐
│        GOLD         │       │ REPORTING VIEWS     │
│ Analytical Tables   │       │ PostgreSQL Views    │
│                     │       │ over Silver Data    │
│ movie_insight       │       │                     │
│ user_insight        │       │ rating_distribution │
│ genre_insight       │       │ rating_time         │
│ yearly_insight      │       │ tag_frequency       │
└──────────┬──────────┘       │ dashboard/detail    │
           │                  │ engagement views    │
           │                  └──────────┬──────────┘
           │                             │
           └──────────────┬──────────────┘
                          ▼
                  ┌───────────────┐
                  │    TABLEAU    │
                  ├───────────────┤
                  │ Dashboard 1   │
                  │ Movie &       │
                  │ Ratings       │
                  ├───────────────┤
                  │ Dashboard 2   │
                  │ Users, Tags & │
                  │ Engagement    │
                  └───────────────┘
```

---

# 12. Final Deliverables

The visualization phase produces:

- Tableau Dashboard 1 — Movie & Ratings Insights
- Tableau Dashboard 2 — User, Tags & Engagement Analysis
- `.twbx` packaged Tableau workbook with extracted data
- Gold analytical datasets
- PostgreSQL reporting views for additional Tableau requirements
- Tableau dashboard documentation

The final dashboards provide an analytical view of MovieLens data across movies, ratings, genres, years, users, tags, and engagement while keeping the visualization layer aligned with the Bronze–Silver–Gold data engineering architecture.

---

# 13. Summary

The Tableau layer represents the final consumption layer of the MovieLens Data Engineering pipeline.

Most high-level analytical metrics are served from precomputed Gold datasets, while additional PostgreSQL views expose cleaned Silver data where Tableau requires a finer level of detail.

This hybrid approach allows the project to preserve:

- Clean data-layer separation
- Reusable Gold analytical assets
- Detailed reporting capability
- Efficient Tableau visualization
- Consistent business metrics
- Scalable handling of the large MovieLens ratings dataset

The final result is an end-to-end analytical pipeline:

```text
MovieLens Data
      ↓
PySpark Processing
      ↓
PostgreSQL Bronze
      ↓
PostgreSQL Silver
      ↓
PostgreSQL Gold + Reporting Views
      ↓
Tableau Dashboards
