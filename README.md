# Methods of Advanced Data Engineering


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