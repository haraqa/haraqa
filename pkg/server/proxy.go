package server

import (
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/pkg/errors"

	"github.com/haraqa/haraqa/internal/headers"
)

func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request, addr string) {
	u, err := url.Parse(addr)
	if err != nil {
		err = errors.Wrap(err, "unable to proxy connection")
		headers.SetError(w, headers.ErrClosed)
		return
	}
	httputil.NewSingleHostReverseProxy(u).ServeHTTP(w, r)
}
