
```
celery -A celery_map_reduce worker --concurrency=4
python3 celery_map_reduce.py
```

Concurrency is 4 to allow direct comparison with rq-mapreduce-demo.
