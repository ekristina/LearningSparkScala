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
