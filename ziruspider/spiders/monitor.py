from urllib.parse import urljoin

import scrapy


class MonitorSpider(scrapy.Spider):
    name = 'monitor'
    allowed_domains = ['www.ziroom.com']
    keywords = [
        '圣馨家园',
        '万方景轩',
        '新纪家园',
        'UHN国际村',
        '光熙家园',
        '光熙门北里',
        '半岛国际',
        '水星园',
        '和泰园',
        '国展新座',
        '太阳国际公馆'
    ]

    def start_requests(self):
        for keyword in self.keywords:
            yield scrapy.Request(url='https://m.ziroom.com/bj/z/z1-r0?pwd=%s&a3&a2&cp=3000TO4200' % keyword, callback=self.parse)

    def parse(self, response):
        if 'display:;' in response.css('.fixed-top').attrib['style']:
            return
        for item in response.css(".app-list-wrapper > ul > li"):
            link = urljoin(response.url, item.css("a").attrib['href'])
            thumb = urljoin(response.url, item.css(
                "a> .house-img > img").attrib['src'])
            title = item.attrib['title']
            desc = item.css(
                "a> .house-content > .desc >span::text").extract()[0]
            desc = desc.replace('\t', ' ').replace('\n', '  ').replace(
                """\t""", " ").replace("""\n""", "  ")
            desc = ' '.join(desc.split())
            yield {
                'link': link,
                'thumb': thumb,
                'title': title,
                'desc': desc,
            }
