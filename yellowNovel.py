
import os
import mini
from yarl import URL
from lxml import etree


host = URL('https://222lie.com/')

class Spider(mini.Spider):
    
    settings = {
        'log_file':'log.txt'
    }
    start_urls = [host.join(URL(f"/t{i:0>2d}/index.html"))for i in range(1,9)]
    
    def __init__(self):
        super().__init__()
        self.path = 'novel'
        mini.check_dir(self.path)
    
    async def parse(self, response, meta=None):
        root = etree.HTML(await response.text())
        cdurl = root.xpath('//a[text()="下一页"]/@href')[0]
        if cdurl != "#":
            url = response.url.join(URL(cdurl))
            yield mini.Request(url)
        ls = root.xpath('//div[@id="colList"]/ul/li/a')
        for i in ls:
            url = i.xpath('./@href')[0]
            name = i.xpath('./h2/text()')[0]
            yield mini.Request(response.url.join(URL(url)), callback=self.parse2, meta=name)
    async def parse2(self, response, meta=None):
        root = etree.HTML(await response.text())
        ls = root.xpath('//div[@class="main-content"]//text()')
        yield {'name':meta,'data':ls}
    
    def parse_item(self, item):
        def _filter(s):
            return s.split()
        path = os.path.join(self.path,_:=item['name'])+'.txt'
        self.logger.info(f'Save start [{_}] path:[{path}]')
        with open(path, 'w') as f:
            ls = list(filter(_filter,item['data']))
            f.writelines(ls)
        self.logger.info(f'Save ok [{_}] path:[{path}]')
Spider().start()