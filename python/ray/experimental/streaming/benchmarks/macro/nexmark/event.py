from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

# An abstract record
class Record(object):
    def __init__(self, **kwds):
        self.__dict__.update(kwds)
        self.event_type = 'Record'

# A watermark with a logical and a generation timestamp
class Watermark(object):
    def __init__(self, event_time, system_time):
        self.event_time = event_time    # The watermak's logical time
        self.system_time = system_time  # The watermark's generation timestamp
        self.event_type = "Watermark"

# An event with an event and a system time. The latter denotes when the
# event enters the system, i.e. when it exits the data source
class Event(object):
    def __init__(self, event_time, system_time=None, extra=None,
                       event_type=None):
        self.system_time = system_time
        self.dateTime = event_time
        self.extra = extra
        self.event_type = event_type

# An Auction event log
class Auction(Event):
    def __init__(self, id=None, item_name=None, description=None,
                       initial_bid=None, reserve=None,
                       date_time=None, expires=None, seller=None,
                       category=None, extra=None):
        Event.__init__(self, date_time, extra=extra, event_type="Auction")
        self.id = id
        self.itemName = item_name
        self.description = description
        self.initialBid = initial_bid
        self.reserve = reserve
        self.expires = expires
        self.seller = seller
        self.category = category

    def __repr__(self):
        to_string = "Id: {}, Item Name: {}, Description: {}, Initial Bid: {},"
        to_string += " Reserve: {}, Expires: {}, Seller: {}, Category: {}"
        to_string += " Datetime: {}, Extra: {}"
        return to_string.format(self.id, self.itemName, self.description,
                                self.initialBid, self.reserve, self.expires,
                                self.seller, self.category, self.dateTime,
                                self.extra)

# An Bid event log
class Bid(Event):
    def __init__(self, auction=None, bidder=None,
                       price=None, date_time=None, extra=None):
        Event.__init__(self, date_time, extra=extra, event_type="Bid")
        self.auction = auction
        self.bidder = bidder
        self.price = price

    def __repr__(self):
        to_string = "Auction: {}, Bidder: {}, Price: {}, Datetime: {}, "
        to_string += "Extra: {}"
        return to_string.format(self.auction, self.bidder, self.price,
                                self.dateTime, self.extra)

# A Person log
class Person(Event):
    def __init__(self, id=None, name=None, email=None, credit_card=None,
                       city=None, state=None, date_time=None, extra=None):
        Event.__init__(self, date_time, extra=extra, event_type="Person")
        self.id = id
        self.name = name
        self.emailAddress = email
        self.creditCard = credit_card
        self.city = city
        self.state = state

    def __repr__(self):
        to_string = "Id: {}, Name: {}, Email: {}, Credit Card: {}, "
        to_string += "City: {}, State: {}, Datetime: {}, Extra: {}"
        return to_string.format(self.id, self.name, self.emailAddress,
                                self.creditCard, self.city, self.state,
                                self.dateTime, self.extra)
