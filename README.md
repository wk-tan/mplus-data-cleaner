# mplus-data-cleaner
Lambda function to clean mplus data during weekdays


# test local docker
```
docker kill $(docker ps -q)
```

```
docker stop mc_container
docker rm mc_container
docker build -t mplus_cleaner .
docker run -p 9000:8080 --name mc_container -v $HOME/.aws:/root/.aws mplus_cleaner 
```

```
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'
```