package json

import (
	"encoding/json"
	"github.com/gagansingh894/go-urlshortner/shortner"
	"github.com/pkg/errors"
)

type Redirect struct{}

func (r *Redirect) Decode(input []byte) (*shortner.Redirect, error) {
	redirect := &shortner.Redirect{}
	err := json.Unmarshal(input, redirect)
	if err != nil {
		return nil, errors.Wrap(err, "serializer.Redirect.Decode")
	}
	return redirect, nil
}

func (r *Redirect) Encode(input *shortner.Redirect) ([]byte, error) {
	rawMsg, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrap(err, "serializer.Redirect.Encode")
	}
	return rawMsg, err
}
