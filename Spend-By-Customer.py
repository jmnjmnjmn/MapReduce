from mrjob.job import MRJob

class SpendByCustomer(MRJob):

    def mapper(self, _, line):
        (customerID, itemID, orderAmount) = line.split(',')
        yield customerID, float(orderAmount)

    def reducer(self, customerID, orders):
        yield customerID, '%04.02f'%float(sum(orders))


if __name__ == '__main__':
    SpendByCustomer.run()
    