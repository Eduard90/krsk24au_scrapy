# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from datetime import datetime
from hashlib import md5
from scrapy import log
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from twisted.enterprise import adbapi
from twisted.internet import reactor

class Krsk24AuScrapyPipeline(object):
    def process_item(self, item, spider):
        return item

class MySQLPipeline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool
        dispatcher.connect(self.spider_opened, signal=signals.spider_opened)
        dispatcher.connect(self.spider_closed, signal=signals.spider_closed)

    def update_users(self, value):
        reactor.stop()

    def spider_opened(self, spider):
        spider.started_on = datetime.now()

    def spider_closed(self, spider):
        work_time = datetime.now() - spider.started_on
        spider.log("~~~~~ WORK TIME: %s" % work_time)

        d = self.dbpool.runQuery("""UPDATE `krsk24au_info_review` SET user_id = (SELECT id FROM krsk24au_info_user WHERE name=user_name LIMIT 1)""")
        d.addCallback(self.update_users)
        reactor.run()

    @classmethod
    def from_settings(cls, settings):
        dbargs = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            passwd=settings['MYSQL_PASSWD'],
            charset='utf8',
            use_unicode=True,
        )
        dbpool = adbapi.ConnectionPool('MySQLdb', **dbargs)
        return cls(dbpool)

    def process_item(self, item, spider):
        # run db query in the thread pool
        d = self.dbpool.runInteraction(self._do_upsert, item, spider)
        d.addErrback(self._handle_error, item, spider)
        # at the end return the item in case of success or failure
        d.addBoth(lambda _: item)
        # return the deferred instead the item. This makes the engine to
        # process next item (according to CONCURRENT_ITEMS setting) after this
        # operation (deferred) has finished.
        return d

    def _do_upsert(self, conn, item, spider):
        """Perform an insert or update."""
        # now = datetime.utcnow().replace(microsecond=0).isoformat(' ')

        item['date_time'] = datetime.strptime(item['date_time'][0], "%d.%m.%Y %H:%M:%S")
        uniq = self._generate_uniqid(item)
        item['uniq'] = uniq

        conn.execute("""SELECT EXISTS(
            SELECT 1 FROM krsk24au_info_review WHERE uniq = %s
        )""", (uniq, ))
        ret = conn.fetchone()[0]

        # conn.execute("""SELECT id FROM krsk24au_info_user WHERE name = %s""", (item['user'], ))
        # user_id = conn.fetchone()
        # spider.log("~~~~~~~ USER_ID: %s" % user_id)
        # user_id = 1

        # if not user_id:
        #     conn.execute("""INSERT INTO krsk24au_info_user (name) VALUES (%s)""", (item['user'], ))
        #     user_id = conn.lastrowid

        if not ret:
            conn.execute("""
                INSERT INTO krsk24au_info_review (uniq, good_id, title, link, date_time, user_name)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (item['uniq'], item['good_id'], item['title'], item['link'], item['date_time'], item['user']))

        # spider.log("~~~~~~~ STORED IN DB: %s" % (uniq))

    def _handle_error(self, failure, item, spider):
        """Handle occurred on db interaction."""
        # do nothing, just log
        log.err(failure)

    def _generate_uniqid(self, item):
        """Generates an unique identifier for a given item."""
        date_time = item['date_time'].strftime("%Y-%m-%d %H:%M:%S")
        id = item['good_id'][0] + date_time
        # if item['good_id'][0] == '3636246':
        #     print md5("%s" % id).hexdigest()
        #     print ("%s - %s", (item['good_id'][0], item['date_time']))
        return md5("%s" % id).hexdigest()