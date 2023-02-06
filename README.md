## Redis Memory Usage Calculate

This script is used to calculate the memory space occupied by a Redis object

### Usage
```bash
$ python calculater.py [-p] type key values....
```
-p: show detail

Example
```bash
$ python calculater.py -p string demo demo
Memory usage detail: 
================ KEY ================
 RedisObject
 type 4
 encoding 4
 refcount 4
 lru 4
 ptr 8
         SDS
         free 4
         len 4
         buf 8
         Array usage 4 bytes
================ VALUE ================
 RedisObject
 type 4
 encoding 4
 refcount 4
 lru 4
 ptr 8
         SDS
         free 4
         len 4
         buf 8
         Array usage 4 bytes
KEY memory usage 44bytes
VALUE memory usage 44bytes
total:  88 bytes
```

You can also introduce the file in the code and call the ```handle(show_detail, type, key, values)``` function
