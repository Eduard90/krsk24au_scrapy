from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.spider import Spider, BaseSpider
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.contrib.loader.processor import TakeFirst
from scrapy.http import Request
from scrapy.contrib.loader import XPathItemLoader, ItemLoader
from scrapy.selector import HtmlXPathSelector, Selector
from krsk24au_scrapy.items import Krsk24AuScrapyItem

USERS = ["lapy", "viewsonic", "Artyx", "Art79", "Vikusya888", "Batia", "len1501", "Stella1", "huckster", "24kupit", "olesia7", "Taka", "serg_em", "ka4eli", "Photoline", "Katerinaa0790", "remaleks", "ivashka", "vovik_125345", "usa-jeans24"]
PAGES = 8
BASE_URL = 'http://24au.ru/reviews/%s/?rate=1'
PAGE_URL = '&page=%s'

class Krsk24AuScrapyLoader(ItemLoader):
    default_output_processor = TakeFirst()

class ReviewsSpider(BaseSpider):
    name = "reviews"
    allowed_domains = ["krsk.24au.ru"]
    # start_urls = [ BASE_URL % s for s in USERS]
    start_urls = [ BASE_URL % s for s in USERS]
    # start_urls = ["http://24au.ru/reviews/dev4/?rate=1"]
    # rules = (
    #     Rule(SgmlLinkExtractor(allow=('http://24au.ru/reviews/dev4/?rate=1')), callback='parse_item'),
    # )

    def __init__(self, category=None, *args, **kwargs):
        super(ReviewsSpider, self).__init__(*args, **kwargs)
        urls = []
        for url in self.start_urls:
            for page in range(1, PAGES+1):
                urls.append(url + PAGE_URL % page)
            # url_ = url + PAGE_URL % s for s in [1, 2]
            # urls.append(url_)

        self.start_urls = urls
        #self.start_urls = [self.start_urls + PAGE_URL % s for s in [1, 2]]

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

    def parse_item(self, response):
        hxs = Selector(response)

        reviews = hxs.xpath('//table[@id="items"]/tr')
        items = []
        for review in reviews:
            item = Krsk24AuScrapyItem()
            item['title'] = review.select('./td[5]/a/text()').extract()
            # yield Krsk24AuScrapyItem(title=title_str, user_url=response.url)


        # return items
        # l = Krsk24AuScrapyLoader(Krsk24AuScrapyItem(), hxs)
        # l.add_value('link', response.url)
        #l.add_xpath('title', '//table[@id="items"]/tbody/tr/')
        # for idx, t in enumerate(l.xpath('//table[@id="items"]')):
        #     l.add_value('link', response.url)



        # return l.load_item()

