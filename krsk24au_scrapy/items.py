# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class Krsk24AuScrapyItem(Item):
    # define the fields for your item here like:
    # name = Field()
    good_id = Field()
    user = Field()
    title = Field()
    link = Field()
    date_time = Field()
    user_url = Field()
    pass
