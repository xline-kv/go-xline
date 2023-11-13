// Copyright 2023 The xline Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xline-kv/go-xline/client"
	"github.com/xline-kv/go-xline/xlog"
	"go.uber.org/zap/zapcore"
)

func TestAuthUser(t *testing.T) {
	xlog.SetLevel(zapcore.WarnLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	xlineClient, err := client.Connect(curpMembers)
	assert.NoError(t, err)
	authClient := xlineClient.Auth

	t.Run("User operations should success in normal path", func(t *testing.T) {
		name1 := "user1"
		password1 := "pwd1"
		password2 := "pwd2"

		authClient.UserAdd(name1, password1)

		_, err := authClient.UserGet(name1)
		assert.NoError(t, err)

		res, _ := authClient.UserList()
		assert.Contains(t, res.Users, name1)

		_, err = authClient.UserChangePassword(name1, password2)
		assert.NoError(t, err)

		authClient.UserDelete(name1)

		_, err = authClient.UserGet(name1)
		assert.Error(t, err)
	})

	t.Run("Role operations should success in normal path", func(t *testing.T) {
		role1 := "role1"
		role2 := "role2"

		_, err := authClient.RoleAdd(role1)
		_, err = authClient.RoleAdd(role2)

		_, err = authClient.RoleGet(role1)
		assert.NoError(t, err)
		_, err = authClient.RoleGet(role2)
		assert.NoError(t, err)

		res, err := authClient.RoleList()
		assert.Equal(t, []string{role1, role2}, res.Roles)

		authClient.RoleDelete(role1)
		authClient.RoleDelete(role2)
	})

	t.Run("User role operations should success in normal path", func(t *testing.T) {
		name1 := "usr1"
		role1 := "role1"
		role2 := "role2"

		authClient.UserAdd(name1, "")
		authClient.RoleAdd(role1)
		authClient.RoleAdd(role2)

		authClient.UserGrantRole(name1, role1)
		authClient.UserGrantRole(name1, role2)

		res, _ := authClient.UserGet(name1)
		assert.Equal(t, []string{role1, role2}, res.Roles)

		authClient.UserRevokeRole(name1, role1)
		authClient.UserRevokeRole(name1, role2)
		authClient.UserDelete(name1)
		authClient.RoleDelete(role1)
		authClient.RoleDelete(role2)
	})

	t.Run("Permission operations should success in normal path", func(t *testing.T) {
		role1 := "role1"

		authClient.RoleAdd(role1)

		authClient.RoleGrantPermission(role1, []byte("123"), nil, client.PermissionType(client.PermRead))
		authClient.RoleGrantPermission(role1, []byte("abc"), nil, client.PermissionType(client.PermWrite))
		authClient.RoleGrantPermission(role1, []byte("hi"), []byte("hjj"), client.PermissionType(client.PermReadWrite))
		authClient.RoleGrantPermission(role1, []byte("pp"), []byte("pq"), client.PermissionType(client.PermWrite))
		authClient.RoleGrantPermission(role1, nil, nil, client.PermissionType(client.PermRead))

		res, _ := authClient.RoleGet(role1)
		assert.Len(t, res.Perm, 5)

		// revoke all permission
		authClient.RoleRevokePermission(role1, []byte("123"), nil)
		authClient.RoleRevokePermission(role1, []byte("abc"), nil)
		authClient.RoleRevokePermission(role1, []byte("hi"), []byte("hjj"))
		authClient.RoleRevokePermission(role1, []byte("pp"), []byte("pq"))
		authClient.RoleRevokePermission(role1, nil, nil)

		res, _ = authClient.RoleGet(role1)
		assert.Empty(t, res.Perm)

		authClient.RoleDelete(role1)
	})
}
