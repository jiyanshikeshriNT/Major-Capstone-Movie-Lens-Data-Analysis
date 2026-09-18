# MovieLens Dataset Profiling

## 1. links.csv
- Columns: `movieId`, `imdbId`, `tmdbId`
- Data types: 
`movieId` - Integer
`imdbId` - String
`tmdbId` - String
- Row count: 87586
- Missing values: There are some missing values in the column `tmdbId`
- Key observations: IMDb IDs may contain leading zeroes, so they should not be treated ordinary numeric values.

## 2. movies.csv
- Columns: `movieId`, `title`, `genres`
- Data types:
`movieId` - Integer
`title` - String
`genres` - String
- Row count: 87586
- Missing values: No missing values were observed during the initial observation.
- Key observations: The movie release year is included inside the `title` field. The `genre` column contains multiple genres for a movie. Genre are seperated using the `|` delimiter.

## 3. ratings.csv
The ratings data is provided as five files:
- `ratings_part1.csv`
- `ratings_part2.csv`
- `ratings_part3.csv`
- `ratings_part4.csv`
- `ratings_part5.csv`

- Columns: `userId`, `movieId`, `rating`, `timestamp`
- Data types:
`userId` - Integer
`movieId` - Integer
`rating` - Numeric
`timestamp` - timestamp
- Row count: Each ratings part contains approximately 6,400,042 rows. Total across the five parts : approx 32 million rows
- Missing values: No missing values were observed during the initial observation.
- Rating range: Ratings are provided on a 0.5 to 5.0 scale in increaments of 0.5.
- Key observations: Each row represents one rating given by a user to a movie. The ratings dataset is the largest dataset in the project.

## 4. tags.csv
- Columns: `userId`, `movieId`, `tag`, `timestamp`
- Data types:
`userId` - Integer
`movieId` - Integer
`tag` - String
`timestamp` - timestamp
- Row count: 2000073
- Missing values: No missing values were observed during the initial observation.
- Key observations: Each row represents a tag assigned by a user to a movie.

## Overall Dataset Summary
- Total movies: 87586
- Total users: Users are identified through the userId column in ratings.csv and tags.csv
- Total ratings: approx 32 million
- Total tags: 2000073