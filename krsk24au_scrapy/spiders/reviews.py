from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.spider import Spider, BaseSpider
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.contrib.loader.processor import TakeFirst
from scrapy.http import Request
from scrapy.contrib.loader import XPathItemLoader, ItemLoader
from scrapy.selector import HtmlXPathSelector, Selector
from krsk24au_scrapy.items import Krsk24AuScrapyItem
from datetime import datetime
from krsk24au_scrapy import settings

BASE_URL = 'http://24au.ru/reviews/%s/?rate=1'
PAGE_URL = '&page=%s'

class Krsk24AuScrapyLoader(ItemLoader):
    default_output_processor = TakeFirst()

class ReviewsSpider(Spider):
    name = "reviews"
    allowed_domains = ["krsk.24au.ru"]
    start_urls = [ BASE_URL % s for s in settings.USERS]

    def __init__(self, category=None, *args, **kwargs):
        super(ReviewsSpider, self).__init__(*args, **kwargs)
        urls = []
        for url in self.start_urls:
            for page in range(1, settings.PAGES+1):
                urls.append(url + PAGE_URL % page)

        self.start_urls = urls

    def parse(self, response):
        hxs = Selector(response)
        reviews = hxs.xpath('//table[@id="items"]/tr')
        items = []
        for review in reviews:
            item = Krsk24AuScrapyItem()
            item['date_time'] = review.xpath('./td[5]/text()').re('([0-9]{2}\.[0-9]{2}\.[0-9]{4} [0-9:]+)')
            item['title'] = review.xpath('./td[5]/a/text()').extract()
            item['link'] = review.xpath('./td[5]/a/@href').extract()
            item['good_id'] = review.xpath('./td[5]/a/@href').re('http:\/\/.*\.ru\/([0-9]+)')
            item['user_url'] = response.url
            items.append(item)

        return items