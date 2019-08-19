Note: the project:kvstore_example is just a demo, no performance, no crash recovery, no reliability.

# env:

Linux/java8



# build:

<!--after clone project-->

mvn clean install

# start kvstore

## start example

cd kvstore_admin/kv_store_admin/target/kv_store_admin

<!-- start example and clear data -->

sh startrace kvstore example 1 64 tcp://0.0.0.0 0

 <!--start example and do not clear data--> 

sh startrace kvstore example 1 64 tcp://0.0.0.01

## start race

cd kvstore_admin/kv_store_admin/target/kv_store_admin

<!-- start race and clear data -->

sh startrace kvstore race 1 64 tcp://0.0.0.0 0

<!-- start race and do not clear data -->

sh startrace kvstore race 1 64 tcp://0.0.0.0 1

# test example

cd kvstore_admin/kv_service_admin/target/kv_service_admin/bin

<!-- the param:1 is kv number(million) -->

sh startrace test example 4 16 tcp://127.0.0.1 0

# test race

cd kvstore_admin/kv_service_admin/target/kv_service_admin/bin

<!-- the param:1 is kv number(million) -->

sh startrace test race 4 16 tcp://127.0.0.1 0


