package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	xlineapi "github.com/xline-kv/go-xline/api/xline"
	"github.com/xline-kv/go-xline/client"
	"github.com/xline-kv/go-xline/xlog"
	"go.uber.org/zap/zapcore"
)

func TestAuthUser(t *testing.T) {
	xlog.SetLevel(zapcore.WarnLevel)

	curpMembers := []string{"172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379"}

	client, err := client.Connect(curpMembers)
	assert.NoError(t, err)
	authClient := client.Auth

	t.Run("User operations should success in normal path", func(t *testing.T) {
		name1 := "user1"
		password1 := "pwd1"
		password2 := "pwd2"

		_, err := authClient.UserAdd(&xlineapi.AuthUserAddRequest{
			Name:     name1,
			Password: password1,
		})
		assert.NoError(t, err)
		_, err = authClient.UserGet(&xlineapi.AuthUserGetRequest{
			Name: name1,
		})
		assert.NoError(t, err)

		res, err := authClient.UserList(&xlineapi.AuthUserListRequest{})
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Contains(t, res.Users, name1)

		_, err = authClient.UserChangePassword(&xlineapi.AuthUserChangePasswordRequest{
			Name:     name1,
			Password: password2,
		})
		assert.NoError(t, err)

		_, err = authClient.UserDelete(&xlineapi.AuthUserDeleteRequest{
			Name: name1,
		})
		assert.NoError(t, err)

		_, err = authClient.UserGet(&xlineapi.AuthUserGetRequest{
			Name: name1,
		})
		assert.Error(t, err)
	})

	t.Run("Role operations should success in normal path", func(t *testing.T) {
		role1 := "role1"
		role2 := "role2"

		_, err := authClient.RoleAdd(&xlineapi.AuthRoleAddRequest{Name: role1})
		assert.NoError(t, err)
		_, err = authClient.RoleAdd(&xlineapi.AuthRoleAddRequest{Name: role2})
		assert.NoError(t, err)

		_, err = authClient.RoleGet(&xlineapi.AuthRoleGetRequest{Role: role1})
		assert.NoError(t, err)
		_, err = authClient.RoleGet(&xlineapi.AuthRoleGetRequest{Role: role2})
		assert.NoError(t, err)

		res, err := authClient.RoleList(&xlineapi.AuthRoleListRequest{})
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, []string{role1, role2}, res.Roles)

		_, err = authClient.RoleDelete(&xlineapi.AuthRoleDeleteRequest{Role: role1})
		assert.NoError(t, err)
		_, err = authClient.RoleDelete(&xlineapi.AuthRoleDeleteRequest{Role: role2})
		assert.NoError(t, err)
	})

	t.Run("User role operations should success in normal path", func(t *testing.T) {
		name1 := "usr1"
		role1 := "role1"
		role2 := "role2"

		_, err := authClient.UserAdd(&xlineapi.AuthUserAddRequest{Name: name1})
		assert.NoError(t, err)
		_, err = authClient.RoleAdd(&xlineapi.AuthRoleAddRequest{Name: role1})
		assert.NoError(t, err)
		_, err = authClient.RoleAdd(&xlineapi.AuthRoleAddRequest{Name: role2})
		assert.NoError(t, err)

		_, err = authClient.UserGrantRole(&xlineapi.AuthUserGrantRoleRequest{
			User: name1,
			Role: role1,
		})
		assert.NoError(t, err)
		_, err = authClient.UserGrantRole(&xlineapi.AuthUserGrantRoleRequest{
			User: name1,
			Role: role2,
		})
		assert.NoError(t, err)

		res, err := authClient.UserGet(&xlineapi.AuthUserGetRequest{Name: name1})
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, []string{role1, role2}, res.Roles)

		_, err = authClient.UserRevokeRole(&xlineapi.AuthUserRevokeRoleRequest{
			Name: name1,
			Role: role1,
		})
		assert.NoError(t, err)
		_, err = authClient.UserRevokeRole(&xlineapi.AuthUserRevokeRoleRequest{
			Name: name1,
			Role: role2,
		})
		assert.NoError(t, err)

		_, err = authClient.UserDelete(&xlineapi.AuthUserDeleteRequest{Name: name1})
		assert.NoError(t, err)
		_, err = authClient.RoleDelete(&xlineapi.AuthRoleDeleteRequest{Role: role1})
		assert.NoError(t, err)
		_, err = authClient.RoleDelete(&xlineapi.AuthRoleDeleteRequest{Role: role2})
		assert.NoError(t, err)
	})

	t.Run("Permission operations should success in normal path", func(t *testing.T) {
		role1 := "role1"

		perm1 := &xlineapi.Permission{
			PermType: xlineapi.Permission_READ,
			Key:      []byte("123"),
		}
		perm2 := &xlineapi.Permission{
			PermType: xlineapi.Permission_WRITE,
			Key:      []byte("abc"),
			RangeEnd: []byte{0},
		}
		perm3 := &xlineapi.Permission{
			PermType: xlineapi.Permission_READWRITE,
			Key:      []byte("hi"),
			RangeEnd: []byte("hjj"),
		}
		perm4 := &xlineapi.Permission{
			PermType: xlineapi.Permission_WRITE,
			Key:      []byte("pp"),
			RangeEnd: []byte("pq"),
		}
		perm5 := &xlineapi.Permission{
			PermType: xlineapi.Permission_READ,
			Key:      []byte{0},
			RangeEnd: []byte{0},
		}

		_, err := authClient.RoleAdd(&xlineapi.AuthRoleAddRequest{Name: role1})
		assert.NoError(t, err)

		_, err = authClient.RoleGrantPermission(&xlineapi.AuthRoleGrantPermissionRequest{
			Name: role1,
			Perm: perm1,
		})
		assert.NoError(t, err)
		_, err = authClient.RoleGrantPermission(&xlineapi.AuthRoleGrantPermissionRequest{
			Name: role1,
			Perm: perm2,
		})
		assert.NoError(t, err)
		_, err = authClient.RoleGrantPermission(&xlineapi.AuthRoleGrantPermissionRequest{
			Name: role1,
			Perm: perm3,
		})
		assert.NoError(t, err)
		_, err = authClient.RoleGrantPermission(&xlineapi.AuthRoleGrantPermissionRequest{
			Name: role1,
			Perm: perm4,
		})
		assert.NoError(t, err)
		_, err = authClient.RoleGrantPermission(&xlineapi.AuthRoleGrantPermissionRequest{
			Name: role1,
			Perm: perm5,
		})
		assert.NoError(t, err)

		res, err := authClient.RoleGet(&xlineapi.AuthRoleGetRequest{Role: role1})
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Len(t, res.Perm, 5)

		// revoke all permission
		_, err = authClient.RoleRevokePermission(&xlineapi.AuthRoleRevokePermissionRequest{
			Role: role1,
			Key:  []byte("123"),
		})
		assert.NoError(t, err)
		_, err = authClient.RoleRevokePermission(&xlineapi.AuthRoleRevokePermissionRequest{
			Role:     role1,
			Key:      []byte("abc"),
			RangeEnd: []byte{0},
		})
		assert.NoError(t, err)
		_, err = authClient.RoleRevokePermission(&xlineapi.AuthRoleRevokePermissionRequest{
			Role:     role1,
			Key:      []byte("hi"),
			RangeEnd: []byte("hjj"),
		})
		assert.NoError(t, err)
		_, err = authClient.RoleRevokePermission(&xlineapi.AuthRoleRevokePermissionRequest{
			Role:     role1,
			Key:      []byte("pp"),
			RangeEnd: []byte("pq"),
		})
		assert.NoError(t, err)
		_, err = authClient.RoleRevokePermission(&xlineapi.AuthRoleRevokePermissionRequest{
			Role:     role1,
			Key:      []byte{0},
			RangeEnd: []byte{0},
		})
		assert.NoError(t, err)

		res, err = authClient.RoleGet(&xlineapi.AuthRoleGetRequest{Role: role1})
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Empty(t, res.Perm)

		_, err = authClient.RoleDelete(&xlineapi.AuthRoleDeleteRequest{Role: role1})
		assert.NoError(t, err)
	})
}
