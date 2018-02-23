# LearningSparkScala

This repository is for learning purposes.

----
### 📁 src/hello_world

Contains training files with simple things like word count or find maximum value.

  📄 *__WordCountBible.scala__* downloads the King James Version bible from [Gutenberg project](http://gutenberg.org) and counts all the words except stop words (which can be found in _english_stop_words.txt_ file). Then prints out top 20 words with a table markdown:
 
 | word | count |
|------|-------|
|lord | 7830|
|god | 4442|
|said | 3999|
|upon | 2748|
|man | 2613|
|israel | 2565|
|son | 2370|
|king | 2257|
|people | 2139|
|came | 2093|
|house | 2024|
|come | 1971|
|one | 1967|
|children | 1802|
|also | 1769|
|day | 1734|
|land | 1718|
|men | 1653|
|let | 1511|
|go | 1492|

📄 *__Precipitation.scala__* takes data from _1800.csv_ file (not in repository) which looks like this:

|station_id|?|data type|amount|?|
|---|---|---|---|---|
|ITE00100554|	18000101|	TMAX|	-75		|	E|	
|ITE00100554|	18000101|	TMIN|	-148	|		E|	
|GM000010962|	18000101|	PRCP|	0		|	E	|
|EZE00100082|	18000101|	TMAX|	-86|			E|	

Then looking for the maximum amount of precipitations and prints out the weather station id and its precipitation value.

```
GM000010962 max precipitation: 305.00
```

📄 *__CustomerOrders.scala__* takes data from _customer-orders.csv_ file 

|customer_id|product_id|amount spent|
|---|---|---|
|44|8602|37.19|

Then counts total amount spent by each customer and prints out customers with spent amount.

```
Customer 45 spent 3309.3804
Customer 79 spent 3790.5698
Customer 96 spent 3924.23
...
```

-----

### 📁 src/MovieRecommendation

Contains files working with [MovieLens data sets](https://grouplens.org/datasets/movielens/)

📄 *__PopularMovies.scala__* counts how many times each movie was rated == how popular the movie is. Doesn't contain movie names, just IDs. Uses _ratings.csv_ file.
Prints out top 20 movies.

📄 *__PopularMoviesNameMapping.scala__* does the same as _PopularMovies.scala_ but maps movie titles with their IDs in results.
Prints out top 20 movies.

Result (using ml-latest dataset:

|Movie|Number of Ratings|
                                                                                |-----|-----------------|
|Forrest Gump (1994)|91921|
|Shawshank Redemption, The (1994)|91082|
|Pulp Fiction (1994)|87901|
|Silence of the Lambs, The (1991)|84078|
|Matrix, The (1999)|77960|
|Star Wars: Episode IV - A New Hope (1977)|77045|
|Jurassic Park (1993)|74355|
|Schindler's List (1993)|67662|
|Braveheart (1995)|66512|
|Toy Story (1995)|66008|
|Star Wars: Episode VI - Return of the Jedi (1983)|62714|
|Terminator 2: Judgment Day (1991)|61836|
|Star Wars: Episode V - The Empire Strikes Back (1980)|61672|
|Fight Club (1999)|60024|
|Raiders of the Lost Ark (Indiana Jones and the Raiders of the Lost Ark) (1981)|59693|
|Usual Suspects, The (1995)|59271|
|American Beauty (1999)|57879|
|Apollo 13 (1995)|57416|
|Independence Day (a.k.a. ID4) (1996)|57232|
|Godfather, The (1972)|57070|
