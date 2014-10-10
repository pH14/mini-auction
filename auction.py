import fdb

fdb.api_version(200)
db = fdb.open()

auction_kv = fdb.directory.create_or_open(db, ('auction-kv',))

auctions_ss = auction_kv['auctions']
bids_ss = auction_kv['bids']
bidders_ss = auction_kv['bidders']

#
# Simplifying assumption: 
#   auction_id == (name, desc), unique
#   bidder_id == (name), unique
#   bid tuples are unique
# 
# Model:
#   auctions#auction_id,highest_bid --> highest bid / winning bid if closed
#   auctions#auction_id,winning_bidder --> current winning bidder
#   auctions#auction_id,num_bids --> # of bids on this item so far
#   auctions#auction_id --> boolean of open or closed
#
#   bids#(auction_id, i) --> bids for auction_id, stored as a kv array
#   bids#(bidder_id, i) --> bids for bidder_id, stored as kv array
#
#   bidders#bidder_id --> name of bidder
#   bidders#bidder_id,num_bids --> number of bids made by bidder_id
#

class Auction(object):
    def __init__(self, name, description):
        self.name = name
        self.description = description

    @property
    def id_tuple(self):
        return (self.name, self.description)

    @property
    def highest_bid_tuple(self):
        return (self.name, self.description, 'highest_bid')

    @property
    def winning_bidder_tuple(self):
        return (self.name, self.description, 'winning_bidder')

    @property
    def num_bids_tuple(self):
        return (self.name, self.description, 'num_bids')

    def get_packed_ids(self, subspace):
        return subspace.pack(self.id_tuple), subspace.pack(self.highest_bid_tuple), \
            subspace.pack(self.num_bids_tuple), subspace.pack(self.winning_bidder_tuple)

    def __str__(self):
        return "Auction(%s, %s)" % (self.name, self.description)

    def __repr__(self):
        return self.__str__()


class Bidder(object):
    def __init__(self, name):
        self.name = name

    @property
    def id_tuple(self):
        return tuple(self.name)

    @property
    def num_bids_tuple(self):
        return (self.name, 'num_bids')

    def get_packed_ids(self, bidder_subspace):
        return bidder_subspace.pack(self.id_tuple), bidder_subspace.pack(self.num_bids_tuple)

    def __str__(self):
        return "Bidder(%s)" % (self.name)

    def __repr__(self):
        return self.__str__()


class Bid(object):
    def __init__(self, bidder, auction, value):
        self.bidder = bidder
        self.auction = auction
        self.value = value

    @property
    def id_tuple(self):
        return (self.bidder, self.auction, self.value)

    @property
    def bidder_id_tuple(self):
        return self.bidder.id_tuple

    @property
    def auction_id_tuple(self):
        return self.auction.id_tuple

    def get_packed_ids(self, bidder_subspace, auction_subspace):
        return bidder_subspace.pack(self.bidder_id_tuple), auction_subspace.pack(self.auction_id_tuple)

    def __str__(self):
        return "Bid(%s, %s, \"%s\", $%.2f)" % (self.bidder.name, self.auction.name, self.auction.description, self.value)

    def __repr__(self):
        return self.__str__()



@fdb.transactional
def add_auction(tr, auction):
    auction_key, highest_bid_key, num_bids_key, winning_bidder_key = auction.get_packed_ids(auctions_ss)

    if tr[auction_key].present(): 
        print "Cannot add auction %s: already exists" % auction
        return

    tr[auction_key] = "OPEN"
    tr[highest_bid_key] = '0'
    tr[winning_bidder_key] = ''
    tr[num_bids_key] = '0'

@fdb.transactional
def close_auction(tr, auction):
    auction_key, _, _, _ = auction.get_packed_ids(auctions_ss)

    if not tr[auction_key].present(): 
        print "Cannot close auction %s: does not exists" % auction
        return

    tr[auction_key] = "CLOSED"

@fdb.transactional
def is_auction_open(tr, auction):
    auction_key, _, _, _ = auction.get_packed_ids(auctions_ss)

    if not tr[auction_key].present():
        print "[%s] Cannot check if auction %s is open: does not exist" % (bid.bidder, bid.auction)
        return

    return tr[auction_key] == "OPEN"

@fdb.transactional
def winning_bid(tr, auction):
    auction_key, highest_bid_key, _, winning_bidder_key = auction.get_packed_ids(auctions_ss)

    if not tr[auction_key].present():
        print "[%s] Cannot check winning bid of auction %s: does not exist" % (bid.bidder, bid.auction)
        return

    return tr[winning_bidder_key], tr[highest_bid_key]

@fdb.transactional
def add_bidder(tr, bidder):
    bidder_key, num_bids_key = bidder.get_packed_ids(bidders_ss)

    if tr[bidder_key].present():
        print "Cannot add bidder %s: already exists" % bidder
        return

    tr[bidder_key] = str(bidder)
    tr[num_bids_key] = '0'

@fdb.transactional
def submit_bid(tr, bid):
    auction_key, highest_bid_key, num_bids_key, winning_bidder_key = bid.auction.get_packed_ids(auctions_ss)
    bidder_key, bidder_num_bids_key = bid.bidder.get_packed_ids(bidders_ss)

    if not tr[auction_key].present():
        print "[%s] Cannot bid on auction %s: does not exist" % (bid.bidder, bid.auction)
        return

    if tr[auction_key] == "CLOSED":
        print "[%s] Cannot bid on auction %s: auction is closed" % (bid.bidder, bid.auction)
        return

    if bid.value <= float(str(tr[highest_bid_key])):
        print "[%s] Cannot bid on auction %s: bid %s is less than %s" % (bid.bidder, bid.auction, bid.value, tr[highest_bid_key])
        return

    if bid.bidder.name == tr[winning_bidder_key]:
        print "[%s] Cannot bid on auction %s: already highest bidder" % (bid.bidder, auction_key)
        return

    # in auctions subspace, set values for the auction
    bid_number = int(tr[num_bids_key]) + 1

    tr[highest_bid_key] = str(bid.value)
    tr[winning_bidder_key] = str(bid.bidder.name)
    tr[num_bids_key] = str(bid_number)

    print "[%s] %s: new highest bid %.2f" % (bid.bidder, bid.auction, bid.value)

    # in bids subspace
    tr[bids_ss[auction_key][bid_number]] = str(bid)

    # -- keep index of bidder's bids, still in bids subspace
    bidder_bid_number = int(tr[bidder_num_bids_key]) + 1
    tr[bidder_num_bids_key] = str(bidder_bid_number)

    tr[bids_ss[bidder_key][bidder_bid_number]] = str(bid)

@fdb.transactional
def bids_for_bidder(tr, bidder):
    bidder_key = bidders_ss.pack(bidder.id_tuple)

    for k, v in tr[bids_ss[bidder_key].range()]:
        yield k, v

@fdb.transactional
def bids_for_auction(tr, auction):
    auction_key = auctions_ss.pack(auction.id_tuple)

    for k, v in tr[bids_ss[auction_key].range()]:
        yield k, v

#################
#### Testing ####
#################

import random
import time
import threading

def running_bidder(bidder, impatience, strategies, strategy_index, auctions):
    add_bidder(db, bidder)

    bid_value = [1 for x in auctions]

    while True:
        for i, auction in enumerate(auctions):
            if not is_auction_open(db, auction):
                return

            winning_bidder, winning_bid_value = winning_bid(db, auction)

            if winning_bidder == bidder.name:
                # we're still winning! hehehe
                continue

            if winning_bid_value > bid_value:
                bid_value[i] = round(strategies[i][strategy_index](float(str(winning_bid_value))), 2)

            submit_bid(db, Bid(bidder, auction, bid_value[i]))

            time.sleep((random.random() / 2.0) * impatience)

def simulate_auction():
    cake       = Auction('cake', 'delicious, delicious cake')
    toothbrush = Auction('toothbrush', 'hardly used')

    auctions = [cake, toothbrush]
    bidders  = [Bidder("Paul"), Bidder("Ori"), Bidder("Nathan")]
    cake_strategies       = [lambda x: x+0.5, lambda x: x+2.25, lambda x: x*1.3]
    toothbrush_strategies = [lambda x: x+4, lambda x: x+1, lambda x: x+1.25]
    strategies = [cake_strategies, toothbrush_strategies]
    impatience = [0.75, 1, 1.25]

    for auction in auctions:
        add_auction(db, auction)

    threads = [
        threading.Thread(target=running_bidder, 
            args=(bidders[i], impatience[i], strategies, i, auctions))
            for i in range(len(bidders))
    ]

    [thr.start() for thr in threads]

    time.sleep(6)
    for auction in auctions:
        close_auction(db, auction)

    [thr.join() for thr in threads]

    print "\n"

    for auction in auctions:
        winning_bidder, winning_bid_value = winning_bid(db, auction)
        congrats = "%s won the %s for $%s! Congratulations!" % (winning_bidder, auction.name, winning_bid_value)

        print '\t', ''.join(["*" for i in range(len(congrats))])
        print '\t', congrats
        print '\t', ''.join(["*" for i in range(len(congrats))])

        print "\n"

    for auction in auctions:
        print "\nBidding history for %s:" % auction
        for _, bid in bids_for_auction(db, auction):
            print "\t", bid

    for bidder in bidders:
        print "\nBidding history for %s" % bidder
        for _, bid in bids_for_bidder(db, bidder):
            print "\t", bid

if __name__ == '__main__':
    del db[auction_kv.range()]
    simulate_auction()
