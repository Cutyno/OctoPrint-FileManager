# based on: https://www.metachris.com/2016/04/python-threadpool/
# which was based on http://code.activestate.com/recipes/577187-python-thread-pool/

import sys
IS_PY2 = sys.version_info < (3, 0)

if IS_PY2:
    from Queue import Queue
else:
    from queue import Queue

from threading import Thread, Event


class Worker(Thread):
    """ Thread executing tasks from a given tasks queue """
    def __init__(self, tasks, stop_event):
        Thread.__init__(self)
        self.tasks = tasks
        self.stop_event = stop_event
        self.daemon = True
        self.start()

    def run(self):
        while not self.stop_event.is_set():
            try:
                func, args, kargs = self.tasks.get(timeout=0.5)
            except:
                # Timeout or Empty exception - check stop_event and continue
                continue
            
            try:
                func(*args, **kargs)
            except Exception as e:
                # An exception happened in this thread
                print(e)
            finally:
                # Mark this task as done, whether an exception happened or not
                self.tasks.task_done()


class ThreadPool:
    """ Pool of threads consuming tasks from a queue """
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        self.stop_event = Event()
        self.workers = []
        for _ in range(num_threads):
            self.workers.append(Worker(self.tasks, self.stop_event))

    def add_task(self, func, *args, **kargs):
        """ Add a task to the queue """
        self.tasks.put((func, args, kargs))

    def map(self, func, args_list):
        """ Add a list of tasks to the queue """
        for args in args_list:
            self.add_task(func, args)

    def wait_completion(self):
        """ Wait for completion of all the tasks in the queue """
        self.tasks.join()
    
    def shutdown(self, wait=True, timeout=10.0):
        """ Shutdown the thread pool gracefully """
        if wait:
            # Wait for pending tasks to complete
            self.tasks.join()
        
        # Signal workers to stop
        self.stop_event.set()
        
        # Wait for all workers to finish
        for worker in self.workers:
            worker.join(timeout=timeout)
