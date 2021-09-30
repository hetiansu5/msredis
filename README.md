[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/github/license/hetiansu5/msredis)](LICENSE)

## Introduction
A master-slave redis client implement based on GoFrame redis

## Keywords
GoFrame redis master slave


## Quick Start

```golang
package main

import (
	"github.com/hetiansu5/msredis"
)

masterName := "cache" // master redis config name
slaveName := "cache_slave" // slave redis config name, can be more
group := msredis.Group(masterName, slaveName)
group.CTX(ctx).Do("GET", "key1")
```


### License
MIT
