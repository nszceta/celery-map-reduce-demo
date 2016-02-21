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
def mapreduce(chunk_size):
    """ A long running task which splits up the input data to many workers """
    # create some sample data for our summation function
    data = []
    for i in range(40):
        x = []
        for j in range(random.randrange(10) + 5):
            x.append(random.randrange(100))
        data.append(x)
    for row in data:
        print('input -> ' + str(row))

    # break up our data into chunks and create a dynamic list of workers
    maps = (map.s(x) for x in partition_all(chunk_size, data))
    mapreducer = chord(maps)(reduce.s())
    return {'chord_id': mapreducer.id}


def create_work(chunk_size):
    """ A fast task for initiating our map function """
    return mapreduce.delay(chunk_size).id


def get_work(chord_id):
    """ A fast task for checking our map result """

    if app.AsyncResult(chord_id).ready():
        result_id = app.AsyncResult(chord_id).get()['chord_id']
    else:
        return {'status': 'pending', 'stage': 1}

    if app.AsyncResult(result_id).ready():
        return {
            'status': 'success',
            'results': app.AsyncResult(result_id).get()}
    else:
        return {'status': 'pending', 'stage': 2}


if __name__ == '__main__':
    my_id = create_work(chunk_size=4)

    for i in range(100):
        time.sleep(1)
        results = get_work(my_id)
        print(results)
        if results['status'] == 'success':
            break
