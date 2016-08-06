from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerSpending")
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    customerId = int(fields[0])
    amountSpent = float(fields[2])
    return (customerId, amountSpent)

input = sc.textFile("file:///sparkcourse/customer-orders.csv")
mappedInputRDD = input.map(extractCustomerPricePairs)
totalByCustomer = mappedInputRDD.reduceByKey(lambda x, y: x + y)
flipped = totalByCustomer.map(lambda (customerId,amountSpent): (amountSpent,customerId))
totalByCustomerSorted = flipped.sortByKey()
results = totalByCustomerSorted.collect()

for result in results:
    print(str(result[1]) + ", {:.2f}".format(result[0]))