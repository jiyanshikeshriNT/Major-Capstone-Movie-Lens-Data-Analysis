# Gold Layer Gap Analysis for Tableau Dashboard

## 1. Objective

The purpose of this document is to validate the existing Gold-layer datasets against the KPI, visual, and filter requirements defined in US-7 – Build Interactive Tableau Dashboard for Stakeholder Reporting.

The analysis identifies:
- Requirements already supported by the existing Gold tables.
- Requirements that are partially supported.
- Missing data fields or Gold objects required before Tableau dashboard development begins.

---

## 2. Existing Gold Layer Inventory

The following Gold tables are currently available:

| Gold Table | Grain / Purpose |
|---|---|
| `gold.movie_insight` | Movie-level rating and tagging insights |
| `gold.user_insight` | User-level rating and engagement insights |
| `gold.genre_insight` | Genre-level rating and movie insights |
| `gold.yearly_insight` | Year-level movie and rating trends |

### Current Row Counts

| Table | Row Count |
|---|---:|
| `gold.movie_insight` | 87,585 |
| `gold.user_insight` | 100,000 |
| `gold.genre_insight` | 19 |
| `gold.yearly_insight` | 142 |

---

## 3. US-7 Requirement Validation

### View 1 – Movie & Ratings Insights

#### KPI Validation

| KPI Requirement | Gold Source | Verified Result | Status |
|---|---|---:|---|
| Total Movies | `gold.movie_insight` | 87,585 | Available |
| Distinct Genres | `gold.genre_insight` | 19 | Available |
| Total Ratings | `gold.movie_insight.TotalRatings` | 32,000,204 | Available |
| Average Rating (Overall) | `gold.movie_insight.AverageRating`, `TotalRatings` | 3.5404 | Available |
| Top Rated Movie (avg + count) | `gold.movie_insight` | Average rating and rating count are available | Available |
| Most Rated Genre | `gold.genre_insight` | Drama | Available |

##### KPI Validation Notes

- `gold.movie_insight` contains 87,585 movie-level records.
- `gold.genre_insight` contains 19 distinct genres.
- The total rating count in Gold is 32,000,204, which exactly matches the row count in `silver.ratings`.
- Overall average rating is calculated as a weighted average using `AverageRating` and `TotalRatings`, resulting in approximately 3.5404.
- A direct average of movie-level averages is not used because every movie does not have the same number of ratings.
- The Top Rated Movie KPI can be derived using `AverageRating` together with `TotalRatings`.
- Drama is currently the most rated genre based on `TotalRatings`.

#### Filter Validation

| Filter Requirement | Existing Gold Support | Status |
|---|---|---|
| Genre | Genre information is available, but not at a common grain across all View 1 datasets | Partial |
| Year | Year information is available in movie and yearly insights, but not consistently across all View 1 datasets | Partial |
| Rating Range | Rating values from 0.5 to 5.0 are available as aggregated rating-count buckets, but not as a common row-level rating field | Partial |

##### Identified Gap

The required Genre, Year, and Rating Range information exists in the Gold layer, but the three dimensions are not available together at a common grain. Therefore, the filters cannot currently be applied consistently to every worksheet in View 1 as required by US-7.

#### Visual Validation

| Visual Requirement | Gold Source | Verification | Status |
|---|---|---|---|
| Average Rating by Genre | `gold.genre_insight` | `Genre` and `AverageRating` are available for all 19 genres | Available |
| Movies by Genre | `gold.genre_insight` | `Genre` and `MovieCount` are available | Available |
| Releases Over Time | `gold.yearly_insight` | `Year` and `NumberOfMoviesReleased` are available | Available |
| Ratings Distribution 1–5 | `gold.genre_insight` | Rating bucket counts from 0.5 to 5.0 are available, but only at genre-level aggregation | Partial |
| Top 10 Movies by Rating Count | `gold.movie_insight` | `MovieTitle` and `TotalRatings` are available; unrated movies contain null rating counts and must be excluded | Available |
| Rating Count vs Average Rating | `gold.movie_insight` | `TotalRatings` and `AverageRating` are available at movie level | Available |
| Ratings Submitted Over Time – Monthly/Yearly | `gold.yearly_insight` | Yearly rating totals are available, but no month or rating timestamp field exists in the current Gold tables | Partial |

##### Visual Validation Notes

- Average Rating by Genre can be directly created using `Genre` and `AverageRating` from `gold.genre_insight`.
- Movies by Genre can be created using `Genre` and `MovieCount`.
- Releases Over Time is supported through `Year` and `NumberOfMoviesReleased` in `gold.yearly_insight`.
- Rating bucket counts from 0.5 to 5.0 are present in `gold.genre_insight`. However, these counts are aggregated by genre. Since a movie can belong to multiple genres, summing these buckets across all genres may duplicate rating activity. Therefore, the overall Ratings Distribution visual is only partially supported by the current Gold model.
- Some movies in `gold.movie_insight` have no ratings and therefore contain a null `TotalRatings` value. These records should be excluded when generating the Top 10 Movies by Rating Count visual.
- The scatter plot can use `TotalRatings` as the rating-count/popularity measure and `AverageRating` as the average-rating measure.
- Yearly ratings submitted over time are supported by `gold.yearly_insight`. Monthly analysis is not currently supported because no month or rating-event timestamp field exists in the Gold layer.

### View 2 – User, Tags & Engagement

#### KPI Validation

| KPI Requirement | Gold Source | Verified Result | Status |
|---|---|---|---|
| Total Users | `gold.user_insight` | 200,948 users | Available |
| Avg Ratings per User | `gold.user_insight.NumberOfMoviesRated` | Approximately 159.25 ratings per user | Available |
| Most Active User | `gold.user_insight.NumberOfMoviesRated` | UserId 175325 with 33,332 ratings | Available |
| Total Tags | `gold.movie_insight.TotalTags` | 2,000,072 tags | Available |
| Most Tagged Movie | `gold.movie_insight.TotalTags` | Pulp Fiction with 6,697 tags | Available |
| Most Used Tag | Existing Gold tag fields | Overall/global tag frequency is not available directly | Gap |

##### KPI Validation Notes

- `gold.user_insight` contains one record per user and supports Total Users, Average Ratings per User, and Most Active User KPIs.
- `gold.movie_insight` contains movie-level tag counts and supports Total Tags and Most Tagged Movie KPIs.
- Pulp Fiction currently has the highest number of tags with 6,697 tag records.
- Several tag-related fields exist in Gold, such as `PopularTag`, `MostUsedTag`, `TotalTagsUsed`, and `DistinctTags`.
- However, these fields are stored at different aggregation levels such as movie, user, genre, or year.
- The current Gold layer does not contain a global tag-frequency dataset that shows each tag together with its total usage count across the complete dataset.
- A validation query on `silver.tags` shows that `sci-fi` is currently the most frequently used tag with 10,996 occurrences.
- Since the Tableau dashboard is expected to use Gold as its reporting source, the Most Used Tag KPI requires additional Gold-layer support.

#### Filter Validation

| Filter Requirement | Existing Gold Support | Status |
|---|---|---|
| User ID | `UserId` is available in `gold.user_insight` only | Partial |
| Genre | `Genre` is available in `gold.genre_insight` only | Partial |
| Year | `ReleaseYear` is available in `gold.movie_insight` and `Year` in `gold.yearly_insight` | Partial |
| Tag Keyword | No row-level `Tag` field exists in the current Gold layer; only aggregated tag fields are available | Partial |

##### Filter Validation Notes

- The required View 2 filters are not available together in a single Gold dataset.
- `UserId` is available in `gold.user_insight`.
- `Genre` is available in `gold.genre_insight`.
- Year is available as `ReleaseYear` in `gold.movie_insight` and as `Year` in `gold.yearly_insight`.
- No exact row-level `Tag` field is available in the Gold layer.
- Existing tag-related columns such as `PopularTag` and `MostUsedTag` are aggregated attributes and cannot represent the complete set of tag values required for a Tag Keyword filter.
- Because User ID, Genre, Year, and Tag are distributed across different Gold tables, the required filters cannot currently be applied consistently to every worksheet in View 2.

### View 2 — Visual Validation

| Visual | Gold Layer Support | Status | Observation |
|---|---|---|---|
| Top 10 Users by Ratings Count | `gold.user_insight` contains `UserId` and `NumberOfMoviesRated`. | Available | Users can be ranked directly by `NumberOfMoviesRated` to create the required bar chart. |
| User Rating Behaviour (Low / Medium / Heavy raters) | `gold.user_insight.NumberOfMoviesRated` is available for every user. | Partial | The underlying metric required for segmentation is available, but the business thresholds defining Low, Medium and Heavy raters are not specified. The categories can be derived in Tableau once the threshold rules are confirmed. |
| Ratings Activity by Day of Week vs Time of Day | No rating timestamp, day-of-week or hour-level field is available in the current Gold insight tables. | Gap | The heatmap requires rating activity at a time-based grain. The existing Gold tables contain aggregated insights and therefore cannot derive day-of-week and time-of-day activity. |
| Top 10 Movies by Number of Tags | `gold.movie_insight` contains `MovieId`, `MovieTitle` and `TotalTags`. | Available | Movies can be ranked by `TotalTags` to create the required bar chart. |
| Movie → User → Tags → Rating Matrix | Required fields are distributed across different Gold tables and no Gold table contains Movie, User, Tag and individual Rating at the same row-level grain. | Gap | A row-level/detail Gold dataset is required if the matrix must be built exclusively from the Gold layer. |
| Tags Count vs Ratings Count per Movie | `gold.movie_insight` contains `MovieId`, `MovieTitle`, `TotalTags` and `TotalRatings` together. | Available | The existing movie-level Gold table directly supports the required scatter plot. |