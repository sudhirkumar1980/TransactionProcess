# TransactionProcess
TransactionProcess count transaction for sliding window of duration 3 minutes wih 2 min slide. Print the output to console. MockNeat tool was used to generate credit card transaction. It uses TransactionTime from source data to for window function.

# Resources
Resources directory contains source file.

# MockNeat Sample Java Code
        MockNeat m = MockNeat.threadLocal();
        final Path path = Paths.get("./CreditTransaction.csv");

        
        m.fmt("#{first},#{last},#{email},#{domain},#{ip},#{creditcard},#{transactiontimestamp}")
         .param("first", m.names().first())
         .param("last", m.names().last())
         .param("email", m.emails())
         .param("domain", m.urls().domain(DomainSuffixType.POPULAR))
         .param("ip", m.ipv4s().types(IPv4Type.CLASS_B, IPv4Type.CLASS_C_NONPRIVATE))
         .param("creditcard", m.creditCards().types(CreditCardType.AMERICAN_EXPRESS, CreditCardType.VISA_16))
         .param("transactiontimestamp",m.longSeq().start(System.currentTimeMillis()/1000- (48 * 60 * 60)).increment(5).cycle(true))
         .list(15000)
         .consume(list -> {
             try { Files.write(path, list, CREATE, WRITE); }
             catch (IOException e) { e.printStackTrace(); }
         });

# Source Data Schema
 val schema = StructType(
      List(
        StructField("First", StringType, true),
        StructField("Last", StringType, true),
        StructField("Email", StringType, true),
        StructField("Domain", StringType, true),
        StructField("IP", StringType, true),
        StructField("CreditCard", StringType, true),
        StructField("TransactionTime", LongType, true)
      )
    )  

# Sample Output
+------------------------------------------+-----+
|window                                    |count|
+------------------------------------------+-----+
|[2020-06-12 23:00:00, 2020-06-12 23:03:00]|36   |
|[2020-06-13 06:52:00, 2020-06-13 06:55:00]|36   |
|[2020-06-12 17:26:00, 2020-06-12 17:29:00]|36   |
|[2020-06-12 18:10:00, 2020-06-12 18:13:00]|36   |
|[2020-06-13 01:30:00, 2020-06-13 01:33:00]|36   |
|[2020-06-13 04:32:00, 2020-06-13 04:35:00]|36   |
|[2020-06-13 06:10:00, 2020-06-13 06:13:00]|36   |
|[2020-06-12 23:42:00, 2020-06-12 23:45:00]|36   |
|[2020-06-12 20:00:00, 2020-06-12 20:03:00]|36   |
|[2020-06-12 12:20:00, 2020-06-12 12:23:00]|36   |
|[2020-06-12 22:44:00, 2020-06-12 22:47:00]|36   |
|[2020-06-12 21:34:00, 2020-06-12 21:37:00]|36   |
|[2020-06-12 21:22:00, 2020-06-12 21:25:00]|36   |
|[2020-06-12 12:08:00, 2020-06-12 12:11:00]|36   |
|[2020-06-12 13:50:00, 2020-06-12 13:53:00]|36   |
|[2020-06-12 18:52:00, 2020-06-12 18:55:00]|36   |
|[2020-06-12 13:12:00, 2020-06-12 13:15:00]|36   |
|[2020-06-13 00:42:00, 2020-06-13 00:45:00]|36   |
|[2020-06-13 06:04:00, 2020-06-13 06:07:00]|36   |
|[2020-06-13 06:58:00, 2020-06-13 07:01:00]|36   |
+------------------------------------------+-----+


