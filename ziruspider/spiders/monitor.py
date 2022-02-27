import scrapy


class MonitorSpider(scrapy.Spider):
    name = 'monitor'
    allowed_domains = ['www.ziroom.com']
    start_urls = [
        'https://www.ziroom.com/z/z1-r0/?p=a3|4|2&qwd=%E5%9C%A3%E9%A6%A8%E5%AE%B6%E5%9B%AD&cp=3000TO4200&isOpen=1']

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        for item in response.css(".Z_list > .Z_list-box > .item"):
            link = item.css(".pic-box>a").attrib['href']
            thumb = item.css(".pic-box>a>img").attrib['src']
            title = item.css(".info-box > .title > a::text").extract()[0]
            desc = item.css(".info-box > .desc > div::text").extract()[0]
            yield {
                'link': link,
                'thumb': thumb,
                'title': title,
                'desc': desc,
            }
