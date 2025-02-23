###### Tutorial = https://www.youtube.com/watch?v=94w6hPk7nkM
###### Interview Questions = https://www.youtube.com/watch?v=fOCiis31Ng4
###### File uploaded to /FileStore/tables/BigMart_Sales.csv
###### File uploaded to /FileStore/tables/drivers.json

### Spark Architecture 
``` Driver Program => Cluster Manager => Worker Node (Executor)Cache, task ```

#### Why Spark?
* in-memory computation
* Lazy evolution
* Fault Tolerance
* Partioning

``` Job (code we write in cell) => Stage => Task ```

#### Lazy Function
##### doesnt execute until called an action