import time
import random
from celery import Celery, chord, chain
from toolz.itertoolz import partition_all, concat

app = Celery('celery_map_reduce',
    broker='redis://localhost', backend='redis://localhost')

app.conf.CELERY_ACCEPT_CONTENT = ['json']
app.conf.CELERY_TASK_SERIALIZER = 'json'
app.conf.CELERY_RESULT_SERIALIZER = 'json'

@app.task
def reduce(mapped):
    """ Reduce worker """
    return list(concat(mapped))


@app.task
def map(data):
    """ Map worker """
    results = []
    for chunk in data:
        # artificial sleep
        time.sleep(random.random())
        results.append(sum(chunk))
    return results


@app.task
def mapreduce(data):
    """ A long running task which splits up the input data to many workers """
    maps = (map.s(x) for x in data)
    mapreduce = chord(maps)(reduce.s())
    return {'asyncresult_id': mapreduce.id}


@app.task
def prepper(input_data):
    """ A long running task which creates the data for our map """
   
    data = []
    for i in range(input_data):
        x = []
        for j in range(random.randrange(10) + 5):
            x.append(random.randrange(100))
        data.append(x)
    for row in data:
        print('input -> ' + str(row))

    chunk_size = 4
    return list(partition_all(chunk_size, data))


def create_work(init_input):
    """ A fast task for initiating our map function """

    c = chain((prepper.s(init_input), mapreduce.s()))()
    mapper_group_id = c.get()['asyncresult_id']
    return mapper_group_id


def get_work(asyncresult_id):
    """ A fast task for checking our map result """

    if app.AsyncResult(asyncresult_id).ready():
        return {
            'status': 'success',
            'results': app.AsyncResult(asyncresult_id).get()}
    else:
        return {'status': 'pending'}


if __name__ == '__main__':
    my_id = create_work(30)

    for i in range(100):
        time.sleep(1)
        results = get_work(my_id)
        print(results)
        if results['status'] == 'success':
            break
