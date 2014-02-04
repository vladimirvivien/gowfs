package gowfs

import "fmt"

func (fs *FileSystem) GetDelegationToken(renewer string) (Token, error){
	return Token{}, fmt.Errorf("Method GetDelegationToken() unimplemented.")
}

func (fs *FileSystem) GetDelegationTokens(renewer string)([]Token, error){
	return []Token{}, fmt.Errorf("Method GetDelegationTokens() unimplemented.")
}

func (fs *FileSystem) RenewToken(token string) (int64, error){
	return -1, fmt.Errorf("Method RenewToken() unimplemented.")
}

func (fs *FileSystem) CancelDelegationToken(token string)(bool, error){
	return false, fmt.Errorf("Method CacnelDelegationToken() unimplemented.")
}
