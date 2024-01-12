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

package client

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/xline-kv/go-xline/api/gen/xline"
	"golang.org/x/crypto/pbkdf2"
)

type Auth interface {
	// AuthEnable enables auth of an etcd cluster.
	AuthEnable() (*AuthEnableResponse, error)

	// AuthDisable disables auth of an etcd cluster.
	AuthDisable() (*AuthDisableResponse, error)

	// AuthStatus returns the status of auth of an etcd cluster.
	AuthStatus() (*AuthStatusResponse, error)

	// Authenticate login and get token
	Authenticate(name string, password string) (*AuthenticateResponse, error)

	// UserAdd adds a new user to an etcd cluster.
	UserAdd(name string, password string) (*AuthUserAddResponse, error)

	// UserAddWithOptions adds a new user to an etcd cluster with some options.
	UserAddWithOptions(name string, password string, opt *UserAddOptions) (*AuthUserAddResponse, error)

	// UserGet gets a detailed information of a user.
	UserGet(name string) (*AuthUserGetResponse, error)

	// UserList gets a list of all users.
	UserList() (*AuthUserListResponse, error)

	// UserDelete deletes a user from an etcd cluster.
	UserDelete(name string) (*AuthUserDeleteResponse, error)

	// UserChangePassword changes a password of a user.
	UserChangePassword(name string, password string) (*AuthUserChangePasswordResponse, error)

	// UserGrantRole grants a role to a user.
	UserGrantRole(user string, role string) (*AuthUserGrantRoleResponse, error)

	// UserRevokeRole revokes a role of a user.
	UserRevokeRole(name string, role string) (*AuthUserRevokeRoleResponse, error)

	// RoleAdd adds a new role to an etcd cluster.
	RoleAdd(name string) (*AuthRoleAddResponse, error)

	// RoleGet gets a detailed information of a role.
	RoleGet(role string) (*AuthRoleGetResponse, error)

	// RoleList gets a list of all roles.
	RoleList() (*AuthRoleListResponse, error)

	// RoleDelete deletes a role.
	RoleDelete(role string) (*AuthRoleDeleteResponse, error)

	// RoleGrantPermission grants a permission to a role.
	RoleGrantPermission(name string, key, rangeEnd []byte, permType PermissionType) (*AuthRoleGrantPermissionResponse, error)

	// RoleRevokePermission revokes a permission from a role.
	RoleRevokePermission(role string, key, rangeEnd []byte) (*AuthRoleRevokePermissionResponse, error)
}

type (
	AuthEnableResponse               xlineapi.AuthEnableResponse
	AuthDisableResponse              xlineapi.AuthDisableResponse
	AuthStatusResponse               xlineapi.AuthStatusResponse
	AuthenticateResponse             xlineapi.AuthenticateResponse
	AuthUserAddResponse              xlineapi.AuthUserAddResponse
	AuthUserGetResponse              xlineapi.AuthUserGetResponse
	AuthUserListResponse             xlineapi.AuthUserListResponse
	AuthUserDeleteResponse           xlineapi.AuthUserDeleteResponse
	AuthUserChangePasswordResponse   xlineapi.AuthUserChangePasswordResponse
	AuthUserGrantRoleResponse        xlineapi.AuthUserGrantRoleResponse
	AuthUserRevokeRoleResponse       xlineapi.AuthUserRevokeRoleResponse
	AuthRoleAddResponse              xlineapi.AuthRoleAddResponse
	AuthRoleGetResponse              xlineapi.AuthRoleGetResponse
	AuthRoleListResponse             xlineapi.AuthRoleListResponse
	AuthRoleDeleteResponse           xlineapi.AuthRoleDeleteResponse
	AuthRoleGrantPermissionResponse  xlineapi.AuthRoleGrantPermissionResponse
	AuthRoleRevokePermissionResponse xlineapi.AuthRoleRevokePermissionResponse

	PermissionType xlineapi.Permission_Type
)

type UserAddOptions xlineapi.UserAddOptions

const (
	PermRead      = xlineapi.Permission_READ
	PermWrite     = xlineapi.Permission_WRITE
	PermReadWrite = xlineapi.Permission_READWRITE
)

// Client for Auth operations.
type authClient struct {
	// The client running the CURP protocol, communicate with all servers.
	curpClient Curp
	// The auth token
	token string
}

// Creates a new `AuthClient`
func NewAuth(curpClient Curp, token string) Auth {
	return &authClient{curpClient: curpClient, token: token}
}

// Enables authentication.
func (c *authClient) AuthEnable() (*AuthEnableResponse, error) {
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthEnableRequest{
			AuthEnableRequest: &xlineapi.AuthEnableRequest{},
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.Er.GetAuthEnableResponse()
	res.Header.Revision = resp.Asr.Revision
	return (*AuthEnableResponse)(resp.Er.GetAuthEnableResponse()), err
}

// Disables authentication.
func (c *authClient) AuthDisable() (*AuthDisableResponse, error) {
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthDisableRequest{
			AuthDisableRequest: &xlineapi.AuthDisableRequest{},
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.Er.GetAuthDisableResponse()
	res.Header.Revision = resp.Asr.Revision
	return (*AuthDisableResponse)(resp.Er.GetAuthDisableResponse()), err
}

// Gets authentication status.
func (c *authClient) AuthStatus() (*AuthStatusResponse, error) {
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthStatusRequest{
			AuthStatusRequest: &xlineapi.AuthStatusRequest{},
		},
	}
	resp, err := c.handleReq(&requestWithToken, true)
	if err != nil {
		return nil, err
	}
	res := resp.Er.GetAuthStatusResponse()
	return (*AuthStatusResponse)(res), err
}

func (c *authClient) Authenticate(name, password string) (*AuthenticateResponse, error) {
	req := &xlineapi.AuthenticateRequest{Name: name, Password: password}
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthenticateRequest{
			AuthenticateRequest: req,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.Er.GetAuthenticateResponse()
	return (*AuthenticateResponse)(res), err
}

// Add an user
func (c *authClient) UserAdd(name, password string) (*AuthUserAddResponse, error) {
	request := &xlineapi.AuthUserAddRequest{Name: name, Password: password}
	if request.Name == "" {
		return nil, errors.New("user name is empty")
	}
	hashedPassword := c.hashPassword([]byte(request.Password))
	request.HashedPassword = hashedPassword
	request.Password = ""

	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthUserAddRequest{
			AuthUserAddRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.Er.GetAuthUserAddResponse()
	return (*AuthUserAddResponse)(res), err
}

// Add an user with option
func (c *authClient) UserAddWithOptions(name, password string, options *UserAddOptions) (*AuthUserAddResponse, error) {
	request := &xlineapi.AuthUserAddRequest{
		Name:     name,
		Password: password,
		Options:  (*xlineapi.UserAddOptions)(options),
	}
	if request.Name == "" {
		return nil, errors.New("user name is empty")
	}
	needPassword := false
	if request.Options != nil {
		needPassword = request.Options.NoPassword
	}
	if needPassword && request.Password == "" {
		return nil, errors.New("password is required but not provided")
	}
	hashedPassword := c.hashPassword([]byte(request.Password))
	request.HashedPassword = hashedPassword
	request.Password = ""

	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthUserAddRequest{
			AuthUserAddRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.Er.GetAuthUserAddResponse()
	return (*AuthUserAddResponse)(res), err
}

// Gets the user info by the user name
func (c *authClient) UserGet(name string) (*AuthUserGetResponse, error) {
	request := &xlineapi.AuthUserGetRequest{Name: name}
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthUserGetRequest{
			AuthUserGetRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.Er.GetAuthUserGetResponse()
	res.Header.Revision = resp.Asr.Revision
	return (*AuthUserGetResponse)(res), err
}

// Lists all users
func (c *authClient) UserList() (*AuthUserListResponse, error) {
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthUserListRequest{
			AuthUserListRequest: &xlineapi.AuthUserListRequest{},
		},
	}
	resp, err := c.handleReq(&requestWithToken, true)
	if err != nil {
		return nil, err
	}
	res := resp.Er.GetAuthUserListResponse()
	return (*AuthUserListResponse)(res), err
}

// Deletes the given key from the key-value store
func (c *authClient) UserDelete(name string) (*AuthUserDeleteResponse, error) {
	request := &xlineapi.AuthUserDeleteRequest{Name: name}
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthUserDeleteRequest{
			AuthUserDeleteRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.Er.GetAuthUserDeleteResponse()
	res.Header.Revision = resp.Asr.Revision
	return (*AuthUserDeleteResponse)(res), err
}

// Change password for an user.
func (c *authClient) UserChangePassword(name, password string) (*AuthUserChangePasswordResponse, error) {
	request := &xlineapi.AuthUserChangePasswordRequest{Name: name, Password: password}
	if request.Password == "" {
		return nil, errors.New("user password is empty")
	}
	hashedPassword := c.hashPassword([]byte(request.Password))
	request.HashedPassword = hashedPassword
	request.Password = ""

	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthUserChangePasswordRequest{
			AuthUserChangePasswordRequest: request,
		},
	}

	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.Er.GetAuthUserChangePasswordResponse()
	res.Header.Revision = resp.Asr.Revision
	return (*AuthUserChangePasswordResponse)(res), err
}

// Grant role for an user
func (c *authClient) UserGrantRole(user, role string) (*AuthUserGrantRoleResponse, error) {
	request := &xlineapi.AuthUserGrantRoleRequest{User: user, Role: role}
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthUserGrantRoleRequest{
			AuthUserGrantRoleRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.Er.GetAuthUserGrantRoleResponse()
	res.Header.Revision = resp.Asr.Revision
	return (*AuthUserGrantRoleResponse)(res), err
}

// Revoke role for an user.
func (c *authClient) UserRevokeRole(name, role string) (*AuthUserRevokeRoleResponse, error) {
	request := &xlineapi.AuthUserRevokeRoleRequest{Name: name, Role: role}
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthUserRevokeRoleRequest{
			AuthUserRevokeRoleRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.Er.GetAuthUserRevokeRoleResponse()
	res.Header.Revision = resp.Asr.Revision
	return (*AuthUserRevokeRoleResponse)(res), err
}

// Adds role.
func (c *authClient) RoleAdd(name string) (*AuthRoleAddResponse, error) {
	request := &xlineapi.AuthRoleAddRequest{Name: name}
	if request.Name == "" {
		return nil, errors.New("role name is empty")
	}

	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthRoleAddRequest{
			AuthRoleAddRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.Er.GetAuthRoleAddResponse()
	res.Header.Revision = resp.Asr.Revision
	return (*AuthRoleAddResponse)(res), err
}

// Gets role
func (c *authClient) RoleGet(role string) (*AuthRoleGetResponse, error) {
	request := &xlineapi.AuthRoleGetRequest{Role: role}
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthRoleGetRequest{
			AuthRoleGetRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, true)
	if err != nil {
		return nil, err
	}
	res := resp.Er.GetAuthRoleGetResponse()
	return (*AuthRoleGetResponse)(res), err
}

// Lists role
func (c *authClient) RoleList() (*AuthRoleListResponse, error) {
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthRoleListRequest{
			AuthRoleListRequest: &xlineapi.AuthRoleListRequest{},
		},
	}
	resp, err := c.handleReq(&requestWithToken, true)
	if err != nil {
		return nil, err
	}
	res := resp.Er.GetAuthRoleListResponse()
	return (*AuthRoleListResponse)(res), err
}

// Deletes role.
func (c *authClient) RoleDelete(role string) (*AuthRoleDeleteResponse, error) {
	request := &xlineapi.AuthRoleDeleteRequest{Role: role}
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthRoleDeleteRequest{
			AuthRoleDeleteRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.Er.GetAuthRoleDeleteResponse()
	res.Header.Revision = resp.Asr.Revision
	return (*AuthRoleDeleteResponse)(res), err
}

// Grants role permission
func (c *authClient) RoleGrantPermission(user string, key, rangeEnd []byte, permType PermissionType) (*AuthRoleGrantPermissionResponse, error) {
	request := &xlineapi.AuthRoleGrantPermissionRequest{
		Name: user, Perm: &xlineapi.Permission{
			Key:      []byte(key),
			RangeEnd: []byte(rangeEnd),
			PermType: xlineapi.Permission_Type(permType),
		},
	}
	if request.Perm == nil {
		return nil, errors.New("permission not given")
	}

	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthRoleGrantPermissionRequest{
			AuthRoleGrantPermissionRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.Er.GetAuthRoleGrantPermissionResponse()
	res.Header.Revision = resp.Asr.Revision
	return (*AuthRoleGrantPermissionResponse)(res), err
}

// Revokes role permission
func (c *authClient) RoleRevokePermission(role string, key, rangeEnd []byte) (*AuthRoleRevokePermissionResponse, error) {
	request := &xlineapi.AuthRoleRevokePermissionRequest{
		Role:     role,
		Key:      []byte(key),
		RangeEnd: []byte(rangeEnd),
	}
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthRoleRevokePermissionRequest{
			AuthRoleRevokePermissionRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.Er.GetAuthRoleRevokePermissionResponse()
	res.Header.Revision = resp.Asr.Revision
	return (*AuthRoleRevokePermissionResponse)(res), err
}

// Send request using fast path
func (c *authClient) handleReq(req *xlineapi.RequestWithToken, useFastPath bool) (*proposeRes, error) {
	cmd := xlineapi.Command{Request: req}

	if useFastPath {
		res, err := c.curpClient.Propose(&cmd, true)
		return res, err
	} else {
		res, err := c.curpClient.Propose(&cmd, false)
		if err != nil {
			return res, err
		} else {
			if res != nil && res.Asr == nil {
				panic("syncResp is always Some when useFastPath is false")
			}
			return res, err
		}
	}
}

// Generate hash of the password
func (c *authClient) hashPassword(password []byte) string {
	const (
		pbkdf2Iter   = 10000
		pbkdf2KeyLen = 32
	)

	salt := make([]byte, 16)
	_, err := rand.Read(salt)
	// This doesn't seems to be fallible
	if err != nil {
		panic(err)
	}

	hashed := pbkdf2.Key(password, salt, pbkdf2Iter, pbkdf2KeyLen, sha256.New)

	_salt := base64.RawStdEncoding.EncodeToString(salt)
	_hashed := base64.RawStdEncoding.EncodeToString(hashed)

	hash := fmt.Sprintf("$pbkdf2-sha256$i=%d,l=%d$%s$%s", pbkdf2Iter, pbkdf2KeyLen, _salt, _hashed)

	return hash
}
