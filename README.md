# Go-Xline

[![Apache 2.0 licensed][apache-badge]][apache-url]
[![codecov][cov-badge]][cov-url]
[![Build Status][actions-badge]][actions-url]

[apache-badge]: https://img.shields.io/badge/license-Apache--2.0-brightgreen
[apache-url]: https://github.com/datenlord/Xline/blob/master/LICENSE
[cov-badge]: https://codecov.io/gh/xline-kv/xline/branch/master/graph/badge.svg
[cov-url]: https://codecov.io/gh/xline-kv/go-xline
[actions-badge]: https://github.com/datenlord/xline/actions/workflows/ci.yml/badge.svg?branch=master
[actions-url]: https://github.com/xline-kv/go-xline/actions

Go-Xline is an official xline client sdk, written in Go.

## Features

`go-xline` runs the CURP protocol on the client side for maximal performance.

## Supported APIs

- KV
  - [x] Put
  - [x] Range
  - [x] Delete
  - [x] Txn
  - [x] Compact
- Auth
  - [x] AuthEnable
  - [x] AuthDisable
  - [x] AuthStatus
  - [x] Authenticate
  - [x] UserAdd
  - [x] UserAddWithOptions
  - [x] UserGet
  - [x] UserList
  - [x] UserDelete
  - [x] UserChangePassword
  - [x] UserGrantRole
  - [x] UserRevokeRole
  - [x] RoleAdd
  - [x] RoleGet
  - [x] RoleList
  - [x] 
  - [x] RoleGrantPermission
  - [x] RoleRevokePermission
- Lease
  - [x] Grant
  - [x] Revoke
  - [x] KeepAlive
  - [x] KeepAliveOnce
  - [x] TimeToLive
  - [x] Leases
- Watch
  - [x] Watch
- Lock
  - [x] Lock
  - [x] Unlock
- Cluster
  - [x] MemberAdd
  - [x] MemberAddAsLearner
  - [x] MemberRemove
  - [x] MemberUpdate
  - [x] MemberList
  - [x] MemberPromote
- Maintenance
  - [ ] Alarm
  - [ ] Status
  - [ ] Defragment
  - [ ] Hash
  - [x] Snapshot
  - [ ] MoveLeader

## Installation

``` bash
go get github.com/xline-kv/go-xline
```

## Quickstart

``` go
import (
	"fmt"

	"github.com/xline-kv/go-xline/client"
)

func ExampleClient() {
	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	cli, err := client.Connect(curpMembers)
	if err != nil {
		panic(err)
	}

	kvCli := cli.KV

	_, err = kvCli.Put([]byte("key"), []byte("value"))
	if err != nil {
		panic(err)
	}

	res, err := kvCli.Range([]byte("key"))
	if err != nil {
		panic(err)
	}

	kv := res.Kvs[0]
	fmt.Println(string(kv.Key), string(kv.Value))
}
```

## Compatibility

We aim to maintain compatibility with each corresponding Xline version, and update this library with each new Xline release.

| go-xline | Xline |
| --- | --- |
| v0.1.0 | v0.6.0 |
| v0.1.1 | v0.6.1 |