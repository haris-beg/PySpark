from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerSpending")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerId = int(fields[0])
    amountSpent = float(fields[2])
    return (customerId, amountSpent)

lines = sc.textFile("file:///sparkcourse/customer-orders.csv")
rdd = lines.map(parseLine)
totalByCustomer = rdd.reduceByKey(lambda x, y: x + y)
totalByCustomerSorted = totalByCustomer.map(lambda (customerId,amountSpent): (amountSpent,customerId)).sortByKey()
results = totalByCustomerSorted.collect()

for result in results:
    print(str(result[1]) + ", {:.2f}".format(result[0]))