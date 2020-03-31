


import os
import inspect
import asyncio
import logging
import hashlib

import aiohttp
from yarl import URL


LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'

settings = {
    'LOG_FILE': None,
    'coroutines'.upper(): 20,
    'LOG_FORMAT' :LOG_FORMAT,
    'LOG_DATEFORMAT': LOG_DATEFORMAT,
    'LOG_LEVE': 'DEBUG',
    'RETRY': 5,
    'HASH_TYPE': 'sha256',
    # 'HASH_KWARGS': {},
}

def check_dir(path):
    path = os.path.abspath(path)
    if not os.path.exists(path) or not os.path.isdir(path):
        os.makedirs(path)

def _init_logger():
    logger = logging.getLogger('Spider')
    logger.setLevel(settings.get('LOG_LEVE', 'DEBUG'))
    a = settings.get('LOG_FORMAT', LOG_FORMAT)
    b = settings.get('LOG_DATEFORMAT', LOG_DATEFORMAT) 
    format = logging.Formatter(a,b)

    steram_hander = logging.StreamHandler()
    steram_hander.setFormatter(format)
    logger.addHandler(steram_hander)
    if file:=settings.get('LOG_FILE', 'log.txt'):
        check_dir(os.path.dirname(file:=os.path.abspath(file)))
        file_hander = logging.FileHandler(file, 'w', 'utf8')
        file_hander.setFormatter(format)
        logger.addHandler(file_hander)
        

class Tasks:
    '''集成：协程任务管理，任务列队管理，过滤器'''
    def __init__(self, spider):
        self.logger = logging.getLogger('Spider.Tasks')
        self.spider = spider
        self._shutdown = False
        self._queue = asyncio.queues.LifoQueue()
        self._tasks = []
        self._filter = set()
  
    def start(self):
        self.loop = asyncio.get_event_loop()
        self.MainTask = self.loop.create_task(self.Main())
        try:
            self.loop.run_until_complete(self.MainTask)
        except asyncio.exceptions.CancelledError:
            self.logger.error('MainTask Cancelled')

        all_tasks = asyncio.all_tasks(self.loop)
        if all_tasks:
            for i in all_tasks:
                i.cancel()
            self.loop.run_until_complete(asyncio.gather(*all_tasks,loop=self.loop, return_exceptions=True))      
        self.loop.run_until_complete(self.loop.shutdown_asyncgens())
        
        self.logger.info('all done')

    async def _start_request(self):
        async for i in self.spider.start_requests():
            await self.put(i)

    async def Main(self):
        '''主携程'''
        conn = aiohttp.TCPConnector(ssl=False,limit=0)
        async with aiohttp.ClientSession(connector=conn) as session:
            Request.session = session
            await self._start_request()
            size = settings.get('coroutines'.upper(),20)
            for i in range(size):
                task = self.loop.create_task(self.JobTask())
                task.add_done_callback(self._callback)
                self._tasks.append(task)
            await self._queue.join()
            self._shutdown = True
            for i in range(size):
                await self._queue.put(None)
            await self._queue.join()
            await asyncio.gather(*self._tasks, loop=self.loop, return_exceptions=True)

    async def JobTask(self):
        '''任务携程'''
        while True:
            request = await self._queue.get()
            if request is not None:
                func = request.callback
                if func is None:
                    func = self.spider.parse
                try:
                    async with request.response as rep:
                        if rep.status != 200:
                            await self.put(request)
                            self._queue.task_done()
                            continue
                        if inspect.isasyncgenfunction(func):
                            async for result in func(rep, request.meta):
                                await self._check_result(result)
                        elif inspect.iscoroutinefunction(func):
                            result = await func(rep, request.meta)
                            await self._check_result(result)
                        else :
                            raise TypeError('回调函数必须是async def')
                except aiohttp.ClientError:
                    self.logger.exception('Request Error:\n')
                    await self.put(request)
                except asyncio.TimeoutError:
                    self.logger.exception('TimeoutError :\n')
                    await self.put(request)
            self._queue.task_done()
            if self._shutdown:
                break

    async def _check_result(self, result):
        if isinstance(result, Request):
            await self.put(result)
        else:
            task = self.loop.run_in_executor(None, self.spider.parse_item, result)

    def _callback(self, task):
        '''任务携程回调'''
        def _exc_do():
            try:
                self._queue.task_done()
            except ValueError:
                pass
                self.logger.warning('queue.task_done ValueError')
        try:
            task.result()
        except asyncio.exceptions.CancelledError:
            _exc_do()
            self.logger.warning(f'Cancelled task:[{task}]')
        except:
            _exc_do()
            self.logger.exception(f'Task Error [task:{task}]')
            self.MainTask.cancel()
            raise
            
    async def put(self, request):
        '''过滤以及放入任务列队中'''
        if request.retry_n >= settings.get('RETRY',5):
            self.logger.warning(f'Retry out [{request}]')
            return 
        if request.filter:
            if (sha256:=request.sha256) not in self._filter:
                await self._queue.put(request)
                self._filter.add(sha256)
        else:
            await self._queue.put(request)


class Request:

    session = None
    
    def __init__(self, url, *, callback=None, method='GET', meta=None, filter=True, **kwargs):
        self.url = url
        self.method = method
        self.callback = callback
        self.meta = meta
        self.filter = filter
        self.kwargs = kwargs
        self.retry_n = 0
        self.logger = logging.getLogger('Spider.Request')
        
    @property
    def response(self):
        self.logger.info(f'{"Staert" if self.retry_n == 0 else "Retry"} resquest:{self}')
        self.retry_n += 1
        return self.session.request(self.method, self.url, **self.kwargs)
        
    def __str__(self):
        return f'<Request method:{self.method} url:{self.url} retry:{self.retry_n}>'
    __repr__ = __str__
    
    @property
    def sha256(self):
        sha256 = hashlib.new(settings.get('HASH_TYPE','sha256'), b'Request sha256')
        sha256.update(bytes(str(self),encoding="utf8"))
        return sha256.hexdigest()


class Spider:

    settings = None
    name = 'Spider'
    start_urls = []
    
    def __init__(self, cfg=None):
        if self.settings:
            self._update_settings(self.settings)
        if cfg:
            self._update_settings(cfg)
        _init_logger()
        self.logger = logging.getLogger(f'Spider.{self.name}')
        
    def _update_settings(self, cfg):
        assert isinstance(cfg, dict),'cfg参数不想是dict对象'
        for k,v in cfg.items():
            settings[k.upper()] = v
            
    async def start_requests(self):
        for url in self.start_urls:
            yield Request(url, filter=False)
            
    def start(self):
        self.logger.info('start crawl')
        t = Tasks(self)
        return t.start()
        
    async def parse(self, response, meta=None):
        pass
        
    def parse_item(self, item):
        self.logger.info(f'Item:[{item}]')

if __name__ == "__main__":


    class MySpider(Spider):
    
        settings = {
        'LOG_FILE': 'log.txt'
        }
        start_urls = ['https://www.baidu.com/']
        
        async def parse(self, response, meta=None):
            yield 'item test'
            yield Request('https://ww.bidu.com/', filter=True, timeout=0)
   
    MySpider().start()