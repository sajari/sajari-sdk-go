package sajari

import "golang.org/x/net/context"

// Credentials is an interface which is implemented by types
// providing credential information used in requests.
type Credentials interface {
	creds() string
}

// KeyCredentials defines a Credential which uses a Key ID-Secret pair.
func KeyCredentials(keyID, keySecret string) Credentials {
	return keyCreds{
		keyID:     keyID,
		keySecret: keySecret,
	}
}

type keyCreds struct {
	keyID, keySecret string
}

func (k keyCreds) creds() string {
	return "keysecret " + k.keyID + " " + k.keySecret
}

type creds struct {
	Credentials
}

func (c creds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": c.Credentials.creds(),
	}, nil
}

func (creds) RequireTransportSecurity() bool {
	return false
}
