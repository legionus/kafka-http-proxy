kafka-http-proxy
================

The project provides a simple HTTP interface for Apache Kafka to store and
read JSON messages.

HTTP API
--------

                 | Method    | Query
---------------- | --------: | -------------
Write message    | **POST**  | `{schema}://{host}/v1/topics/{topic}/{partition}`
Receive messages | **GET**   | `{schema}://{host}/v1/topics/{topic}/{partition}?offset={offset}&limit={limit}`


How to Install
--------------

Run:

    $ go get -u github.com/legionus/kafka-http-proxy


LICENSE
-------

The project is licensed under the GNU General Public License.

Links
-----

More about the Apache Kafka: http://kafka.apache.org/
