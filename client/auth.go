package client

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/google/uuid"
	xlineapi "github.com/xline-kv/go-xline/api/xline"
	"golang.org/x/crypto/pbkdf2"
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
func (c *authClient) AuthEnable() (*xlineapi.AuthEnableResponse, error) {
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
	res := resp.CommandResp.GetAuthEnableResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return res, err
}

// Disables authentication.
func (c *authClient) AuthDisable() (*xlineapi.AuthDisableResponse, error) {
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
	res := resp.CommandResp.GetAuthDisableResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return res, err
}

// Gets authentication status.
func (c *authClient) AuthStatus() (*xlineapi.AuthStatusResponse, error) {
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
	res := resp.CommandResp.GetAuthStatusResponse()
	return res, err
}

func (c *authClient) Authenticate(request *xlineapi.AuthenticateRequest) (*xlineapi.AuthenticateResponse, error) {
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthenticateRequest{
			AuthenticateRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, false)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthenticateResponse()
	return res, err
}

// Add an user
func (c *authClient) UserAdd(request *xlineapi.AuthUserAddRequest) (*xlineapi.AuthUserAddResponse, error) {
	if request.Name == "" {
		return nil, errors.New("User name is empty")
	}
	needPassword := false
	if request.Options != nil {
		needPassword = request.Options.NoPassword
	}
	if needPassword && request.Password == "" {
		return nil, errors.New("Password is required but not provided")
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
	res := resp.CommandResp.GetAuthUserAddResponse()
	return res, err
}

// Gets the user info by the user name
func (c *authClient) UserGet(request *xlineapi.AuthUserGetRequest) (*xlineapi.AuthUserGetResponse, error) {
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
	res := resp.CommandResp.GetAuthUserGetResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return res, err
}

// Lists all users
func (c *authClient) UserList(request *xlineapi.AuthUserListRequest) (*xlineapi.AuthUserListResponse, error) {
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthUserListRequest{
			AuthUserListRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, true)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthUserListResponse()
	return res, err
}

// Deletes the given key from the key-value store
func (c *authClient) UserDelete(request *xlineapi.AuthUserDeleteRequest) (*xlineapi.AuthUserDeleteResponse, error) {
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
	res := resp.CommandResp.GetAuthUserDeleteResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return res, err
}

// Change password for an user.
func (c *authClient) UserChangePassword(request *xlineapi.AuthUserChangePasswordRequest) (*xlineapi.AuthUserChangePasswordResponse, error) {
	if request.Password == "" {
		return nil, errors.New("User password is empty")
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
	res := resp.CommandResp.GetAuthUserChangePasswordResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return res, err
}

// Grant role for an user
func (c *authClient) UserGrantRole(request *xlineapi.AuthUserGrantRoleRequest) (*xlineapi.AuthUserGrantRoleResponse, error) {
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
	res := resp.CommandResp.GetAuthUserGrantRoleResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return res, err
}

// Revoke role for an user.
func (c *authClient) UserRevokeRole(request *xlineapi.AuthUserRevokeRoleRequest) (*xlineapi.AuthUserRevokeRoleResponse, error) {
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
	res := resp.CommandResp.GetAuthUserRevokeRoleResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return res, err
}

// Adds role.
func (c *authClient) RoleAdd(request *xlineapi.AuthRoleAddRequest) (*xlineapi.AuthRoleAddResponse, error) {
	if request.Name == "" {
		return nil, errors.New("Role name is empty")
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
	res := resp.CommandResp.GetAuthRoleAddResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return res, err
}

// Gets role
func (c *authClient) RoleGet(request *xlineapi.AuthRoleGetRequest) (*xlineapi.AuthRoleGetResponse, error) {
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
	res := resp.CommandResp.GetAuthRoleGetResponse()
	return res, err
}

// Lists role
func (c *authClient) RoleList(request *xlineapi.AuthRoleListRequest) (*xlineapi.AuthRoleListResponse, error) {
	requestWithToken := xlineapi.RequestWithToken{
		Token: &c.token,
		RequestWrapper: &xlineapi.RequestWithToken_AuthRoleListRequest{
			AuthRoleListRequest: request,
		},
	}
	resp, err := c.handleReq(&requestWithToken, true)
	if err != nil {
		return nil, err
	}
	res := resp.CommandResp.GetAuthRoleListResponse()
	return res, err
}

// Deletes role.
func (c *authClient) RoleDelete(request *xlineapi.AuthRoleDeleteRequest) (*xlineapi.AuthRoleDeleteResponse, error) {
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
	res := resp.CommandResp.GetAuthRoleDeleteResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return res, err
}

// Grants role permission
func (c *authClient) RoleGrantPermission(request *xlineapi.AuthRoleGrantPermissionRequest) (*xlineapi.AuthRoleGrantPermissionResponse, error) {
	if request.Perm == nil {
		return nil, errors.New("Permission not given")
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
	res := resp.CommandResp.GetAuthRoleGrantPermissionResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return res, err
}

// Revokes role permission
func (c *authClient) RoleRevokePermission(request *xlineapi.AuthRoleRevokePermissionRequest) (*xlineapi.AuthRoleRevokePermissionResponse, error) {
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
	res := resp.CommandResp.GetAuthRoleRevokePermissionResponse()
	res.Header.Revision = resp.SyncResp.Revision
	return res, err
}

// Send request using fast path
func (c authClient) handleReq(req *xlineapi.RequestWithToken, useFastPath bool) (*ProposeResponse, error) {
	proposeId := c.generateProposeId()
	cmd := xlineapi.Command{Request: req, ProposeId: proposeId}

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
