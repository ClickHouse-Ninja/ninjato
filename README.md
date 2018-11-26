# Ninjato - horizontally scalable metrics server build on top of ClickHouse.

# @TODO

- [ ] Documentation  
- [ ] Reports examples (views and queries)
- [ ] Grafana example dashboard
- [ ] Prometheus remote RW storage 

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
|magic number|byte (146)|

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
