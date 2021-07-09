package gowfs

import "fmt"
import "net/http"

func (fs *FileSystem) GetDelegationToken(renewer string) (Token, error) {
	params := map[string]string{"op": OP_GETDELEGATIONTOKEN, "renewer": renewer}
	rsp, reqErr := fs.sendHttpRequest("GET", nil, &params, nil, false)
	if reqErr != nil {
		return Token{}, reqErr
	}
	defer rsp.Body.Close()
	hdfsData, err := responseToHdfsData(rsp)
	if err != nil {
		return Token{}, err
	}
	return hdfsData.Token, nil
}

func (fs *FileSystem) GetDelegationTokens(renewer string) ([]Token, error) {
	params := map[string]string{"op": OP_GETDELEGATIONTOKENS, "renewer": renewer}
	rsp, reqErr := fs.sendHttpRequest("GET", nil, &params, nil, false)
	if reqErr != nil {
		return nil, reqErr
	}
	defer rsp.Body.Close()
	hdfsData, err := responseToHdfsData(rsp)
	if err != nil {
		return nil, err
	}
	return hdfsData.Tokens.Token, nil
}

func (fs *FileSystem) RenewDelegationToken(token string) (int64, error) {
	params := map[string]string{"op": OP_RENEWDELEGATIONTOKEN, "token": token}
	rsp, reqErr := fs.sendHttpRequest("PUT", nil, &params, nil, false)
	if reqErr != nil {
		return -1, reqErr
	}
	defer rsp.Body.Close()
	hdfsData, err := responseToHdfsData(rsp)
	if err != nil {
		return -1, err
	}
	return hdfsData.Long, nil

}

func (fs *FileSystem) CancelDelegationToken(token string) (bool, error) {
	params := map[string]string{"op": OP_CANCELDELEGATIONTOKEN, "token": token}
	rsp, reqErr := fs.sendHttpRequest("PUT", nil, &params, nil, false)
	if reqErr != nil {
		return false, reqErr
	}
	defer rsp.Body.Close()
	if rsp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("SetPermission() - server returned unexpected status, token not cancelled.")
	}
	return true, nil
}
