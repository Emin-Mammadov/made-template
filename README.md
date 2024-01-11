# Relationship between Bitcoin and S&P 500 Prices

This project aims to explore the relationship between Bitcoin, a leading cryptocurrency, and the S&P 500, a key indicator of the US stock market's health. Given Bitcoin's growing prominence and its perceived role as a market sentiment indicator, understanding its correlation with traditional financial markets is of great interest. This analysis will use historical price data of both Bitcoin and the S&P 500 to identify potential correlations, divergences, and patterns that may exist between these two distinct but increasingly interconnected markets.

## Kaggle Authentication
Here kaggle is the dataset provider. For that we need to use their authentication to retrieve a data set.
Using this [link](https://www.kaggle.com/settings) you can download a kaggle.json and place it inside the `/project/kaggle.json`
directory.

**Filepath:** `/project/kaggle.json`

```
{
	"username":"emi***********7",
	"key":"baee7*****************455d"
}
```

## Give an execute permissions to the script file of the pipeline
```
chmod +x ./project/pipeline.sh
```

## Run pipeline
```
./project/pipeline.sh
```
