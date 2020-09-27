package headers

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/pkg/errors"
)

func TestErrors(t *testing.T) {
	// no error
	testError(t, nil, http.StatusOK)

	// bad topic
	testError(t, ErrTopicDoesNotExist, http.StatusPreconditionFailed)
	testError(t, ErrTopicAlreadyExists, http.StatusPreconditionFailed)

	// bad request
	testError(t, ErrInvalidHeaderSizes, http.StatusBadRequest)
	testError(t, ErrInvalidMessageID, http.StatusBadRequest)
	testError(t, ErrInvalidMessageLimit, http.StatusBadRequest)
	testError(t, ErrInvalidTopic, http.StatusBadRequest)
	testError(t, ErrInvalidBodyMissing, http.StatusBadRequest)
	testError(t, ErrInvalidBodyJSON, http.StatusBadRequest)

	// no content
	testError(t, ErrNoContent, http.StatusNoContent)

	// websocket error
	testError(t, ErrInvalidWebsocket, http.StatusBadRequest)

	// closed error
	testError(t, ErrClosed, http.StatusServiceUnavailable)

	// undefined error
	testError(t, errors.New("some new error"), http.StatusInternalServerError)

	// empty error
	if err := ReadErrors(map[string][]string{HeaderErrors: {}}); err != nil {
		t.Fatal(err)
	}
	if err := ReadErrors(map[string][]string{HeaderErrors: {""}}); err != nil {
		t.Fatal(err)
	}
	if err := ReadErrors(map[string][]string{HeaderErrors: {"", errInvalidTopic}}); err != ErrInvalidTopic {
		t.Fatal(err)
	}
}

func testError(t *testing.T, errIn error, code int, wrapped ...struct{}) {
	if len(wrapped) == 0 {
		testError(t, errors.Wrap(errIn, "some wrapping message"), code, struct{}{})
	}
	w := httptest.NewRecorder()
	SetError(w, errIn)
	resp := w.Result()
	if code != resp.StatusCode {
		t.Fatal(code, resp.StatusCode)
	}

	errOut := ReadErrors(resp.Header)
	if errors.Cause(errIn) != errOut {
		// check unique errors
		if code == http.StatusInternalServerError && errors.Cause(errIn).Error() == errOut.Error() {
			return
		}
		t.Fatal(errors.Cause(errIn), errOut)
	}
}

func TestSizes(t *testing.T) {
	testSize(t, map[string][]string{}, nil, ErrInvalidHeaderSizes)
	testSize(t, map[string][]string{HeaderSizes: {}}, nil, ErrInvalidHeaderSizes)
	testSize(t, map[string][]string{HeaderSizes: {"blue"}}, nil, ErrInvalidHeaderSizes)
	testSize(t, map[string][]string{HeaderSizes: {"123"}}, []int64{123}, nil)
	testSize(t, map[string][]string{HeaderSizes: {"123", "456"}}, []int64{123, 456}, nil)

	h := http.Header{}
	SetSizes([]int64{}, h)
	testSize(t, h, nil, ErrInvalidHeaderSizes)
	SetSizes([]int64{123}, h)
	testSize(t, h, []int64{123}, nil)
	SetSizes([]int64{123, 456}, h)
	testSize(t, h, []int64{123, 456}, nil)

}

func testSize(t *testing.T, header http.Header, sizes []int64, err error) {
	s, e := ReadSizes(header)
	if err != e {
		t.Fatal(header, err, e)
	}
	if !reflect.DeepEqual(sizes, s) {
		t.Fatal(header, sizes, s)
	}
}
