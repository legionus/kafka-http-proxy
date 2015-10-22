## kafka-http-proxy [![Build Status](https://travis-ci.org/legionus/kafka-http-proxy.svg?branch=master)](https://travis-ci.org/legionus/kafka-http-proxy)

The project provides a simple HTTP interface for Apache Kafka to store and
read JSON messages.


### HTTP API

Url Structure: `{schema}://{host}/v1/topics/{topic}/{partition}`  
Method: **POST**  
Description: Write message  


Url Structure: `{schema}://{host}/v1/topics/{topic}/{partition}?offset={offset}&limit={limit}`  
Method: **GET**  
Description: Receive messages  


Url Structure: `{schema}://{host}/v1/info/topics`  
Method: **GET**  
Description: Obtain topic list  


Url Structure: `{schema}://{host}/v1/info/topics/{topic}`  
Method: **GET**  
Description: Obtain information about all partitions in topic  


Url Structure: `{schema}://{host}/v1/info/topics/{topic}/{partition}`  
Method: **GET**  
Description: Obtain information about partition  


### How to Install

    $ go get -u github.com/legionus/kafka-http-proxy

### LICENSE

The project is licensed under the GNU General Public License.

### Links

More about the Apache Kafka: http://kafka.apache.org/
