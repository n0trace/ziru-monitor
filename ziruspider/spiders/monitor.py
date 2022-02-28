import scrapy


class MonitorSpider(scrapy.Spider):
    name = 'monitor'
    allowed_domains = ['www.ziroom.com']
    start_urls = [
        'https://www.ziroom.com/z/z1-r0/?p=a3|4|2&qwd=%E5%9C%A3%E9%A6%A8%E5%AE%B6%E5%9B%AD&cp=3000TO4200&isOpen=1',  # 土
        'https://www.ziroom.com/z/z1-r0/?p=a3%7C4%7C2&qwd=%E4%B8%87%E6%96%B9%E6%99%AF%E8%BD%A9&cp=3000TO4200&isOpen=1',  # 万方
        'https://www.ziroom.com/z/z1-r0/?p=a3%7C4%7C2&qwd=%E6%96%B0%E7%BA%AA%E5%AE%B6%E5%9B%AD&cp=3000TO4200&isOpen=1',  # 新纪
        'https://www.ziroom.com/z/z1-r0/?p=a3%7C4%7C2&qwd=UHN&cp=3000TO4200&isOpen=1',  # UHN
        'https://www.ziroom.com/z/z1-r0/?p=a3%7C4%7C2&qwd=%E5%85%89%E7%86%99%E5%AE%B6%E5%9B%AD&cp=3000TO4200&isOpen=1',  # 光熙家园
        'https://www.ziroom.com/z/z1-r0/?p=a3%7C4%7C2&qwd=%E5%85%89%E7%86%99%E9%97%A8%E5%8C%97%E9%87%8C&cp=3000TO4200&isOpen=1',  # 光熙门北里
        'https://www.ziroom.com/z/z1-r0/?p=a3%7C4%7C2&qwd=%E5%8D%8A%E5%B2%9B%E5%9B%BD%E9%99%85%E5%85%AC%E5%AF%93&cp=3000TO4200&isOpen=1',  # 半岛国际
        'https://www.ziroom.com/z/z1-r0/?p=a3%7C4%7C2&qwd=%E6%B0%B4%E6%98%9F%E5%9B%AD&cp=3000TO4200&isOpen=1',  # 水星园
    ]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        for item in response.css(".Z_list > .Z_list-box > .item"):
            link = item.css(".pic-box>a").attrib['href']
            thumb = item.css(".pic-box>a>img").attrib['data-original']
            title = item.css(".info-box > .title > a::text").extract()[0]
            desc = item.css(".info-box > .desc > div::text").extract()[0]
            yield {
                'link': link,
                'thumb': thumb,
                'title': title,
                'desc': desc,
            }
