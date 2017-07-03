
import asyncio
from queue import Queue
from functools import partial
from aiohttp import web
from threading import Thread
from collections import deque
import json
import pprint
from collections import defaultdict

class Task:
    '''
        A task which will hold a coroutine executable when running the callable protocol.
        It may have children tasks held in the subTasks list
    '''
    __slots__ = ('id', 'coro', 'onDone', 'cancelFunc', 'state', 'result', 'exception', 'parentId', 'successCondition', 'subTasks', 'name', 'description')

    _lastid = 1
    
    (kNotProcessed, kProcessing, kSuccess, kFailed, kError, kNeedValidation) = range(6)

    def __init__(self, coro, name='', description='', onDone=None, cancelFunc=None, successCondition=None, taskid=None):
        if taskid is None:
            taskid = Task._lastid
            Task._lastid += 1
        self.coro = coro
        self.onDone = onDone
        self.id = taskid
        self.cancelFunc = cancelFunc
        self.state = self.kNotProcessed
        self.result = None
        self.successCondition = successCondition
        self.exception = None
        self.subTasks = []
        self.parentId = 0

    def __repr__(self):
        return 'Task(id=%r, %r, state=%r)' % (self.id, self.coro, self.state)

    def __str__(self):
        return str({'TaskId':str(self.id), 'State' : str(self.state)})

    async def __call__(self):
        self.state = self.kProcessing
        try:
            self.result = await self.coro()
            if self.successCondition:
                self.state = self.kSuccess if self.successCondition(self.result) else self.kFailed
            else:
                self.state = self.kNeedValidation
        except Exception as ex:
            self.state = self.kError
            self.exception = ex

    def __iter__(self):
        '''
            When iterating first return current task, then go through all child tasks and so on until a leaf is reached
        '''
        yield self
        for child in self.subTasks:
            for task in child:
                yield task

    def addTask(self, task):
        self.subTasks.append(task)
        task.parentId = self.id
        return task

    def getChildTasks(self, flag=None):
        return [task for task in self.filterByFlag(flag)]

    def filterByFlag(self, flag):
        for task in self.subTasks:
            if flag and task.state == flag:
                yield task
            elif not flag:
                yield task

    def getExecutionQueue(self):
        '''
            Use the iterator to build up the execution queue. This will give the execution order for the current task and its children.
            It will process tasks on the same level in the tree in parallel
        '''
        executionDict = defaultdict(list)
        for task in self:
            executionDict[task.parentId].append(task)
        
        q = Queue()
        for key in executionDict:
            q.put(executionDict[key])

        return q

class TaskRunner:
    '''
        Wrapper over a task to help run it. Can be used as a context manager.
    '''
    __slots__ =('task', 'shutdown', 'executionQueue', 'currentTaskBatch', 'runnerId')

    _lastid = 1
    
    def __init__(self):
        self.runnerId = TaskRunner._lastid
        TaskRunner._lastid += 1
        self.task = None
        self.shutdown = False
        self.executionQueue = Queue()

    
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.stop()

    def stop(self):
        self.shutdown = True

    def pushEvents(self, task):
        self.task = task
        self.executionQueue = self.task.getExecutionQueue()

    async def run(self):
        while not self.shutdown:
            while not self.executionQueue.empty():
                self.currentTaskBatch = self.executionQueue.get()
                print("processing: %s" % ','.join([str(task) for task in self.currentTaskBatch]))
                await asyncio.gather(*[task() for task in self.currentTaskBatch])
            else:
                await asyncio.sleep(1)

    async def handle(self, request):
        return web.Response(status=200, content_type='application/json', 
            text=json.dumps([str(item) for item in self.task]))


class TaskThread:
    '''
        Separate thread used for holding a task runner. It will run on its own asyncio loop
    '''
    _lastid = 1

    def __init__(self, name, description):
        self.taskRunner = TaskRunner()
        self.workerThread = None
        self.apiThread = None
        self.name = name
        self.description = description
        self.threadId = TaskThread._lastid
        TaskThread._lastid += 1

    def toJson(self):
        return {'id': self.threadId, 'name':self.name, 'description': self.description}

    def addTasks(self, tasks):
        self.taskRunner.pushEvents(tasks)

    def startLoop(self, loop):
        asyncio.set_event_loop(loop)
        loop.create_task(self.taskRunner.run())
        loop.run_forever()       

    def start(self):
        workerLoop = asyncio.new_event_loop()
        
        self.workerThread = Thread(target=self.startLoop, args=(workerLoop,))
        self.workerThread.start()
        
class TaskManager():
    '''
        Main class for managing one or more TaskThread instances. Each will run o a separate thread with its asyncio loop.
        Exposes simple http api to see running tasks. More for testing.
    '''
    def __init__(self):
        self.taskThreadList = []
        
        self.app = web.Application()
        apiLoop = asyncio.get_event_loop()
        self.apiThread = Thread(target=self.startApi, args=(apiLoop,self.app,))
        self.apiThread.start()

    def filterById(self, thId):
        for runner in self.taskThreadList:
            if runner.threadId == thId:
                yield runner

    async def handleTaskRunners(self, request):
        return web.Response(status=200, content_type='application/json', 
            text=json.dumps([threadrunner.toJson() for threadrunner in self.taskThreadList]))

    async def handleTaskRunner(self, request):
        taskRunnerId = int(request.match_info.get('id', 0))
        taskRunnerThread = next(self.filterById(taskRunnerId), None)
        if taskRunnerThread:
            return await taskRunnerThread.taskRunner.handle(request)
        else:
            raise web.HTTPNotFound(text="Runner not found")

    def startApi(self, loop, app):
        app.router.add_get('/taskrunners', self.handleTaskRunners)    
        app.router.add_get('/taskrunner/{id}', self.handleTaskRunner)
        handler = app.make_handler()
        f = loop.create_server(handler, '0.0.0.0', 8080)
        srv = loop.run_until_complete(f)
        loop.run_forever()
        
    def addTask(self, task, name, description):
        taskThread = TaskThread(name, description)
        taskThread.addTasks(task)
        taskThread.start()
        self.taskThreadList.append(taskThread)
        

async def sleeper():
    await asyncio.sleep(5)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    
    manager = TaskManager()

    
    taskList = []

    task1 = Task(sleeper)
    task2 = Task(sleeper)
    task3 = Task(sleeper)
    
    task1.addTask(task2)
    task1.addTask(task3)
    
    task4 = Task(sleeper)
    task5 = Task(sleeper)

    task2.addTask(task4)
    task2.addTask(task5)

    task6 = Task(sleeper)
    task7 = Task(sleeper)
    task8 = Task(sleeper)
    
    task3.addTask(task6)
    
    task6.addTask(task7)
    
    task7.addTask(task8)

    task4.addTask(Task(sleeper))
    
    
    manager.addTask(task1,"Take1", "Description for take 1")
    #manager.addTask(task1,"Take2", "Description for take 2")

    
    while True:
        pass
    
    
