# Ninjato

## Protocol 

```
+-- LZ4 compressed message ----------------------------+
|  +-----------------------+---------+-----+---------+ |
|  | service name (String) | point 1 | ... | point n | |
|  +-----------------------+---------+-----+---------+ |
+------------------------------------------------------+
```

**Point** 

|field|type|
|-----|----|
|label|String|
|value|Float64|
|timestamp|Int32|
|tags len|UInt8|
|tags keys|N String|
|tags values|N String|
|fields len|UInt8|
|fields keys|N String|
|fields values|N Float64|
|control byte| magic byte (146) |

**String** 
```
+------+---------+
| len  | Uint8   |
+------|---------+
| data | N bytes |
+------+---------+
```

## Clients 

|language|repository|
|-----|----|
|Go|[github.com/ClickHouse-Ninja/ninjato](https://github.com/ClickHouse-Ninja/ninjato)|
