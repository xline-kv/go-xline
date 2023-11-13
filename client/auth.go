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

	"github.com/google/uuid"
	pb "github.com/xline-kv/go-xline/api/xline"
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
	AuthEnableResponse               pb.AuthEnableResponse
	AuthDisableResponse              pb.AuthDisableResponse
	AuthStatusResponse               pb.AuthStatusResponse
	AuthenticateResponse             pb.AuthenticateResponse
	AuthUserAddResponse              pb.AuthUserAddResponse
	AuthUserGetResponse              pb.AuthUserGetResponse
	AuthUserListResponse             pb.AuthUserListResponse
	AuthUserDeleteResponse           pb.AuthUserDeleteResponse
	AuthUserChangePasswordResponse   pb.AuthUserChangePasswordResponse
	AuthUserGrantRoleResponse        pb.AuthUserGrantRoleResponse
	AuthUserRevokeRoleResponse       pb.AuthUserRevokeRoleResponse
	AuthRoleAddResponse              pb.AuthRoleAddResponse
	AuthRoleGetResponse              pb.AuthRoleGetResponse
	AuthRoleListResponse             pb.AuthRoleListResponse
	AuthRoleDeleteResponse           pb.AuthRoleDeleteResponse
	AuthRoleGrantPermissionResponse  pb.AuthRoleGrantPermissionResponse
	AuthRoleRevokePermissionResponse pb.AuthRoleRevokePermissionResponse

	PermissionType pb.Permission_Type
)

type UserAddOptions pb.UserAddOptions

const (
	PermRead      = pb.Permission_READ
	PermWrite     = pb.Permission_WRITE
	PermReadWrite = pb.Permission_READWRITE
)

// Client for Auth operations.
type authClient struct {
	// Name of the AuthClient, which will be used in CURP propose id generation
	name string
	// The client running the CURP protocol, communicate with all servers.
	curpClient curpClient
	// The auth token
	token string
}

// Creates a new `AuthClient`
func newAuthClient(name string, curpClient curpClient, token string) authClient {
	return authClient{
		name:       name,
		curpClient: curpClient,
		token:      token,
	}
}

// Enables authentication.
func (c *authClient) AuthEnable() (*AuthEnableResponse, error) {
	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_AuthEnableRequest{
			AuthEnableRequest: &pb.AuthEnableRequest{},
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthEnableResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return (*AuthEnableResponse)(resp.CommandResp.GetAuthEnableResponse()), err
}

// Disables authentication.
func (c *authClient) AuthDisable() (*AuthDisableResponse, error) {
	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_AuthDisableRequest{
			AuthDisableRequest: &pb.AuthDisableRequest{},
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthDisableResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return (*AuthDisableResponse)(resp.CommandResp.GetAuthDisableResponse()), err
}

// Gets authentication status.
func (c *authClient) AuthStatus() (*AuthStatusResponse, error) {
	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_AuthStatusRequest{
			AuthStatusRequest: &pb.AuthStatusRequest{},
		},
	}
	resp, err := c.handleReq(&requestWithToken, true)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthStatusResponse()
	return (*AuthStatusResponse)(res), err
}

func (c *authClient) Authenticate(name, password string) (*AuthenticateResponse, error) {
	req := &pb.AuthenticateRequest{Name: name, Password: password}
	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_AuthenticateRequest{
			AuthenticateRequest: req,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthenticateResponse()
	return (*AuthenticateResponse)(res), err
}

// Add an user
func (c *authClient) UserAdd(name, password string) (*AuthUserAddResponse, error) {
	request := &pb.AuthUserAddRequest{Name: name, Password: password}
	if request.Name == "" {
		return nil, errors.New("user name is empty")
	}
	hashedPassword := c.hashPassword([]byte(request.Password))
	request.HashedPassword = hashedPassword
	request.Password = ""

	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_AuthUserAddRequest{
			AuthUserAddRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthUserAddResponse()
	return (*AuthUserAddResponse)(res), err
}

// Add an user with option
func (c *authClient) UserAddWithOption(name, password string, options *UserAddOptions) (*AuthUserAddResponse, error) {
	request := &pb.AuthUserAddRequest{
		Name:     name,
		Password: password,
		Options:  (*pb.UserAddOptions)(options),
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

	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_AuthUserAddRequest{
			AuthUserAddRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthUserAddResponse()
	return (*AuthUserAddResponse)(res), err
}

// Gets the user info by the user name
func (c *authClient) UserGet(name string) (*AuthUserGetResponse, error) {
	request := &pb.AuthUserGetRequest{Name: name}
	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_AuthUserGetRequest{
			AuthUserGetRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthUserGetResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return (*AuthUserGetResponse)(res), err
}

// Lists all users
func (c *authClient) UserList() (*AuthUserListResponse, error) {
	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_AuthUserListRequest{
			AuthUserListRequest: &pb.AuthUserListRequest{},
		},
	}
	resp, err := c.handleReq(&requestWithToken, true)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthUserListResponse()
	return (*AuthUserListResponse)(res), err
}

// Deletes the given key from the key-value store
func (c *authClient) UserDelete(name string) (*AuthUserDeleteResponse, error) {
	request := &pb.AuthUserDeleteRequest{Name: name}
	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_AuthUserDeleteRequest{
			AuthUserDeleteRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthUserDeleteResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return (*AuthUserDeleteResponse)(res), err
}

// Change password for an user.
func (c *authClient) UserChangePassword(name, password string) (*AuthUserChangePasswordResponse, error) {
	request := &pb.AuthUserChangePasswordRequest{Name: name, Password: password}
	if request.Password == "" {
		return nil, errors.New("user password is empty")
	}
	hashedPassword := c.hashPassword([]byte(request.Password))
	request.HashedPassword = hashedPassword
	request.Password = ""

	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_AuthUserChangePasswordRequest{
			AuthUserChangePasswordRequest: request,
		},
	}

	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthUserChangePasswordResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return (*AuthUserChangePasswordResponse)(res), err
}

// Grant role for an user
func (c *authClient) UserGrantRole(user, role string) (*AuthUserGrantRoleResponse, error) {
	request := &pb.AuthUserGrantRoleRequest{User: user, Role: role}
	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_AuthUserGrantRoleRequest{
			AuthUserGrantRoleRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthUserGrantRoleResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return (*AuthUserGrantRoleResponse)(res), err
}

// Revoke role for an user.
func (c *authClient) UserRevokeRole(name, role string) (*AuthUserRevokeRoleResponse, error) {
	request := &pb.AuthUserRevokeRoleRequest{Name: name, Role: role}
	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_AuthUserRevokeRoleRequest{
			AuthUserRevokeRoleRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthUserRevokeRoleResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return (*AuthUserRevokeRoleResponse)(res), err
}

// Adds role.
func (c *authClient) RoleAdd(name string) (*AuthRoleAddResponse, error) {
	request := &pb.AuthRoleAddRequest{Name: name}
	if request.Name == "" {
		return nil, errors.New("role name is empty")
	}

	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_AuthRoleAddRequest{
			AuthRoleAddRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthRoleAddResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return (*AuthRoleAddResponse)(res), err
}

// Gets role
func (c *authClient) RoleGet(role string) (*AuthRoleGetResponse, error) {
	request := &pb.AuthRoleGetRequest{Role: role}
	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_AuthRoleGetRequest{
			AuthRoleGetRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, true)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthRoleGetResponse()
	return (*AuthRoleGetResponse)(res), err
}

// Lists role
func (c *authClient) RoleList() (*AuthRoleListResponse, error) {
	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_AuthRoleListRequest{
			AuthRoleListRequest: &pb.AuthRoleListRequest{},
		},
	}
	resp, err := c.handleReq(&requestWithToken, true)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthRoleListResponse()
	return (*AuthRoleListResponse)(res), err
}

// Deletes role.
func (c *authClient) RoleDelete(role string) (*AuthRoleDeleteResponse, error) {
	request := &pb.AuthRoleDeleteRequest{Role: role}
	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_AuthRoleDeleteRequest{
			AuthRoleDeleteRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthRoleDeleteResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return (*AuthRoleDeleteResponse)(res), err
}

// Grants role permission
func (c *authClient) RoleGrantPermission(user string, key, rangeEnd []byte, permType PermissionType) (*AuthRoleGrantPermissionResponse, error) {
	request := &pb.AuthRoleGrantPermissionRequest{
		Name: user, Perm: &pb.Permission{
			Key:      []byte(key),
			RangeEnd: []byte(rangeEnd),
			PermType: pb.Permission_Type(permType),
		},
	}
	if request.Perm == nil {
		return nil, errors.New("permission not given")
	}

	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_AuthRoleGrantPermissionRequest{
			AuthRoleGrantPermissionRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthRoleGrantPermissionResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return (*AuthRoleGrantPermissionResponse)(res), err
}

// Revokes role permission
func (c *authClient) RoleRevokePermission(role string, key, rangeEnd []byte) (*AuthRoleRevokePermissionResponse, error) {
	request := &pb.AuthRoleRevokePermissionRequest{
		Role:     role,
		Key:      []byte(key),
		RangeEnd: []byte(rangeEnd),
	}
	requestWithToken := pb.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &pb.RequestWithToken_AuthRoleRevokePermissionRequest{
			AuthRoleRevokePermissionRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthRoleRevokePermissionResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return (*AuthRoleRevokePermissionResponse)(res), err
}

// Send request using fast path
func (c authClient) handleReq(req *pb.RequestWithToken, useFastPath bool) (*ProposeResponse, error) {
	proposeId := c.generateProposeId()
	cmd := pb.Command{Request: req, ProposeId: proposeId}

	if useFastPath {
		res, err := c.curpClient.propose(&cmd, true)
		return res, err
	} else {
		res, err := c.curpClient.propose(&cmd, false)
		if err != nil {
			return res, err
		} else {
			if res != nil && res.SyncResp == nil {
				panic("syncResp is always Some when useFastPath is false")
			}
			return res, err
		}
	}
}

// Generate hash of the password
func (c authClient) hashPassword(password []byte) string {
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

// Generate a new `ProposeId`
func (c authClient) generateProposeId() string {
	return fmt.Sprintf("%s-%s", c.name, uuid.New().String())
}
