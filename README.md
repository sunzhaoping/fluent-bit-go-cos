# fluent-bit-go-cls

An output plugin (Go) for TencentYun COS

### Build

```
go build -buildmode=c-shared -o fluent-bit-go-cos.so .
```

### Run as fluent-bit
```
fluent-bit -c example/fluent.conf -e fluent-bit-go-cos.so
```


### Example Config

```
[OUTPUT]
        Name             fluent-bit-go-cos
        Match            *
        CosPath          "cosn://test/ddd" 
```